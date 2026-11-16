from flask import Flask, request, jsonify, g
import os
import smtplib
import threading
import time
import uuid
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from supabase import create_client

app = Flask(__name__)
SERVER_API_VERSION = "6.1-INTEGRIDAD-DISTRIBUIDA"

# --- CONFIGURACIÓN DE SUPABASE ---
SUPABASE_URL = os.environ.get("SUPABASE_URL", "").strip()
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "").strip()

# Limpiar posibles comillas o espacios extras en las variables de entorno
if SUPABASE_URL.startswith('"') and SUPABASE_URL.endswith('"'):
    SUPABASE_URL = SUPABASE_URL[1:-1]
if SUPABASE_KEY.startswith('"') and SUPABASE_KEY.endswith('"'):
    SUPABASE_KEY = SUPABASE_KEY[1:-1]

supabase = None
if SUPABASE_URL and SUPABASE_KEY:
    try:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        print("Conexión con Supabase inicializada correctamente.")
    except Exception as e:
        print(f"Error crítico al crear el cliente de Supabase: {e}")

# --- MEMORIA LOCAL EXCLUSIVA PARA SESIONES ACTIVAS ---
sesiones_activas = {}
operaciones_lock = threading.RLock()

# Bloqueo distribuido compartido por TODAS las instancias Render.
# Requiere crear las funciones SQL incluidas en el archivo SQL complementario.
POS_LOCK_NAME = "pos_operaciones_global"
POS_LOCK_LEASE_SECONDS = 180
POS_LOCK_WAIT_SECONDS = 12
POS_LOCK_RENEW_SECONDS = 30

# --- BLOQUEO DISTRIBUIDO SUPABASE (COMPARTIDO ENTRE RENDER 1 Y RENDER 2) ---
def _rpc_bool(response):
    """Interpreta respuestas booleanas de PostgREST/Supabase sin asumir un formato único."""
    data = getattr(response, "data", response)
    if isinstance(data, bool):
        return data
    if isinstance(data, list) and data:
        first = data[0]
        if isinstance(first, bool):
            return first
        if isinstance(first, dict):
            return next((v for v in first.values() if isinstance(v, bool)), False)
    if isinstance(data, dict):
        return next((v for v in data.values() if isinstance(v, bool)), False)
    return False

def _rpc_lock(function_name, owner_id):
    if not supabase:
        raise RuntimeError("Supabase no está conectado; se bloqueó la modificación para proteger los datos")
    response = supabase.rpc(function_name, {
        "p_lock_name": POS_LOCK_NAME,
        "p_owner_id": owner_id,
        "p_lease_seconds": POS_LOCK_LEASE_SECONDS,
    }).execute()
    return _rpc_bool(response)

def _renovar_bloqueo(owner_id, detener):
    """Renueva la concesión mientras una petición modificadora sigue trabajando."""
    while not detener.wait(POS_LOCK_RENEW_SECONDS):
        try:
            if not _rpc_lock("pos_renovar_bloqueo", owner_id):
                app.logger.error("No se pudo renovar el bloqueo distribuido %s", owner_id)
                return
        except Exception:
            app.logger.exception("Error renovando el bloqueo distribuido")
            return

@app.before_request
def adquirir_bloqueo_para_escrituras():
    """Serializa escrituras de todas las instancias; GET y prueba SMTP no bloquean."""
    if request.method not in ("POST", "PUT", "PATCH", "DELETE") or request.path == "/correo/prueba":
        return None
    if not supabase:
        return jsonify({"error": "Supabase no está disponible. La modificación fue detenida para proteger los datos."}), 503
    if not operaciones_lock.acquire(timeout=POS_LOCK_WAIT_SECONDS):
        return jsonify({"error": "El servidor está ocupado procesando otra operación. Intenta nuevamente."}), 503
    owner_id = uuid.uuid4().hex
    try:
        limite = time.monotonic() + POS_LOCK_WAIT_SECONDS
        adquirido = False
        while time.monotonic() < limite:
            if _rpc_lock("pos_adquirir_bloqueo", owner_id):
                adquirido = True
                break
            time.sleep(0.25)
        if not adquirido:
            operaciones_lock.release()
            return jsonify({"error": "Otra caja está procesando una operación. No se modificaron los datos; vuelve a intentar."}), 503
        detener = threading.Event()
        renovador = threading.Thread(target=_renovar_bloqueo, args=(owner_id, detener), daemon=True)
        g.pos_lock_owner = owner_id
        g.pos_lock_stop = detener
        g.pos_lock_thread = renovador
        renovador.start()
        return None
    except Exception as e:
        operaciones_lock.release()
        app.logger.exception("No se pudo adquirir el bloqueo distribuido")
        return jsonify({"error": "No se pudo asegurar el bloqueo compartido en Supabase. Operación cancelada.", "detalle": str(e)}), 503

@app.after_request
def liberar_bloqueo_para_escrituras(response):
    owner_id = getattr(g, "pos_lock_owner", None)
    if owner_id:
        detener = getattr(g, "pos_lock_stop", None)
        if detener:
            detener.set()
        try:
            supabase.rpc("pos_liberar_bloqueo", {
                "p_lock_name": POS_LOCK_NAME,
                "p_owner_id": owner_id,
            }).execute()
        except Exception:
            app.logger.exception("No se pudo liberar el bloqueo; expirará automáticamente")
        finally:
            operaciones_lock.release()
    return response

# Versión compartida del catálogo. No debe vivir solo en la memoria de un Render.
catalogo_version = 1

def obtener_catalogo_version_compartida():
    try:
        valor = leer_json("server_catalogo_version", 1)
        return max(1, int(valor))
    except Exception:
        return max(1, int(catalogo_version))

def marcar_catalogo_cambiado():
    global catalogo_version
    actual = obtener_catalogo_version_compartida()
    nueva = actual + 1
    if not guardar_json("server_catalogo_version", nueva):
        app.logger.error("No se pudo actualizar server_catalogo_version; se conserva versión local")
    catalogo_version = nueva
    return nueva


# --- FUNCIONES AUXILIARES DE PERSISTENCIA EN SUPABASE ---
def leer_json(key_name, default):
    if not supabase:
        print(f"Advertencia: Supabase no está conectado. Usando default para {key_name}")
        return default
    try:
        response = supabase.table("app_storage").select("data").eq("Key", key_name).execute()
        if response.data and len(response.data) > 0:
            return response.data[0]["data"]
    except Exception as e:
        print(f"Error leyendo {key_name} de Supabase: {e}")
    return default

def guardar_json(key_name, datos):
    if not supabase:
        print(f"Advertencia: Supabase no está conectado. No se pudo guardar {key_name}")
        return False
    try:
        supabase.table("app_storage").upsert({"Key": key_name, "data": datos}).execute()
        return True
    except Exception as e:
        print(f"Error guardando {key_name} en Supabase: {e}")
        return False

# --- FUNCIÓN AUXILIAR PARA ENVIAR CORREOS SMTP ---
def leer_productos_estricto():
    """
    Lectura de inventario SIN fallback a [].
    Si Supabase falla, se aborta la operación para impedir que un error de lectura
    convierta accidentalmente el catálogo completo en una lista vacía.
    """
    if not supabase:
        raise RuntimeError("Supabase no está conectado")
    try:
        response = supabase.table("app_storage").select("data").eq("Key", "server_productos").execute()
        if response.data and len(response.data) > 1:
            raise RuntimeError("Existen varias filas para Key=server_productos; lectura ambigua cancelada")
        if not response.data:
            # Catálogo inexistente es distinto de catálogo vacío. Solo se acepta [] si
            # la clave existe realmente en app_storage.
            raise RuntimeError("No existe la clave server_productos en app_storage")
        data = response.data[0].get("data")
        if not isinstance(data, list):
            raise RuntimeError("server_productos no contiene una lista válida")
        return data
    except Exception as e:
        raise RuntimeError(f"No se pudo leer server_productos de Supabase: {e}")

def respaldar_productos_antes_de_cambio(productos, motivo):
    """Guarda una copia del catálogo antes de cualquier mutación."""
    if not isinstance(productos, list):
        return False
    try:
        from datetime import datetime
        respaldo = {
            "fecha": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "motivo": str(motivo),
            "cantidad_productos": len(productos),
            "productos": productos
        }
        # Respaldo actual.
        if not guardar_json("server_productos_ultimo_respaldo", respaldo):
            return False

        # Historial limitado para poder recuperar una versión anterior.
        historial = leer_json("server_productos_historial", [])
        if not isinstance(historial, list):
            historial = []
        historial.append(respaldo)
        historial = historial[-20:]
        return guardar_json("server_productos_historial", historial)
    except Exception as e:
        print(f"Error creando respaldo previo de productos: {e}")
        return False

def guardar_productos_seguro(productos_nuevos, productos_antes=None, motivo="cambio"):
    """
    Único punto de escritura del catálogo. Nunca permite reducir la cantidad de
    productos ni aceptar un catálogo que pierda una identidad existente.
    """
    if not isinstance(productos_nuevos, list):
        return False, "Catálogo inválido"
    if productos_antes is not None:
        antes = {str(p.get("id", "")).strip() for p in productos_antes if isinstance(p, dict) and str(p.get("id", "")).strip()}
        despues = {str(p.get("id", "")).strip() for p in productos_nuevos if isinstance(p, dict) and str(p.get("id", "")).strip()}
        if len(productos_nuevos) < len(productos_antes):
            return False, "Protección anti-pérdida: el catálogo no puede reducir su cantidad de productos"
        faltantes = antes - despues
        if faltantes:
            return False, f"Protección anti-pérdida: faltan {len(faltantes)} ID(s) existentes en el nuevo catálogo"
    if not respaldar_productos_antes_de_cambio(productos_antes if productos_antes is not None else productos_nuevos, motivo):
        return False, "No se pudo crear respaldo de seguridad"
    if not guardar_json("server_productos", productos_nuevos):
        return False, "No se pudo guardar el catálogo"
    marcar_catalogo_cambiado()
    return True, "ok"

def enviar_correo_smtp(destinatario, emisor, password, asunto, cuerpo):
    try:
        msg = MIMEMultipart()
        msg['From'] = emisor
        msg['To'] = destinatario
        msg['Subject'] = asunto
        msg.attach(MIMEText(cuerpo, 'plain'))

        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(emisor, password)
        server.sendmail(emisor, destinatario, msg.as_string())
        server.quit()
        return True
    except Exception as e:
        print(f"Error enviando correo: {e}")
        return False

# --- RUTAS DE CONFIGURACIÓN ---
@app.route('/configuracion', methods=['GET', 'POST'])
def manejar_configuracion():
    config_default = {
        "url_servidor": "https://sistema-pos-20cm.onrender.com",
        "tasa_bcv": 36.50,
        "correo_destino": os.environ.get("CORREO_DESTINO", ""),
        "correo_emisor": os.environ.get("CORREO_EMISOR", ""),
        "pass_emisor": os.environ.get("PASS_EMISOR", ""),
        "actualizar_tasa_auto": "Sí"
    }
    if request.method == 'POST':
        data = request.json
        guardar_json("server_config", data)
        return jsonify({"status": "success", "message": "Configuración actualizada"}), 200
    else:
        config = leer_json("server_config", config_default)
        return jsonify(config), 200

@app.route('/correo/prueba', methods=['POST'])
def probar_correo():
    data = request.json or {}
    correo_destino = str(data.get("correo_destino", "")).strip()
    correo_emisor = str(data.get("correo_emisor", "")).strip()
    pass_emisor = str(data.get("pass_emisor", "")).strip()

    if not correo_destino or not correo_emisor or not pass_emisor:
        return jsonify({"error": "Debes indicar correo destino, correo emisor y clave de aplicación."}), 400

    asunto = "Prueba de correo - Sistema POS"
    cuerpo = (
        "Este es un correo de prueba del Sistema POS.\n\n"
        "Si recibiste este mensaje, la configuración SMTP está funcionando correctamente."
    )
    if enviar_correo_smtp(correo_destino, correo_emisor, pass_emisor, asunto, cuerpo):
        return jsonify({"status": "success", "message": f"Correo de prueba enviado correctamente a {correo_destino}."}), 200
    return jsonify({"error": "No se pudo enviar el correo. Revisa el correo emisor, la clave de aplicación y la configuración SMTP de Gmail."}), 500

@app.route('/productos/respaldo', methods=['POST'])
def respaldar_productos():
    data = request.json or {}
    productos = data.get("productos")
    if not isinstance(productos, list):
        return jsonify({"error": "El respaldo debe contener una lista de productos."}), 400

    # Este endpoint SOLO CREA/ACTUALIZA UNA COPIA DE SEGURIDAD.
    # Nunca modifica ni elimina server_productos.
    from datetime import datetime
    respaldo = {
        "fecha": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "cantidad_productos": len(productos),
        "productos": productos
    }
    if guardar_json("server_productos_ultimo_respaldo", respaldo):
        return jsonify({
            "status": "success",
            "message": f"Respaldo de {len(productos)} productos guardado correctamente. El catálogo original no fue modificado ni borrado."
        }), 200
    return jsonify({"error": "No se pudo guardar el respaldo en Supabase."}), 500

# --- RUTAS DE USUARIOS ---
@app.route('/usuarios', methods=['GET', 'POST'])
def manejar_usuarios():
    usuarios_default = {
        "Administrador": ["admin123", "Administrador"],
        "Caja 1": ["caja123", "Cajero"],
        "Caja 2": ["caja456", "Cajero"],
        "Caja 3": ["caja789", "Cajero"]
    }
    if request.method == 'POST':
        data = request.json
        guardar_json("server_usuarios", data)
        return jsonify({"status": "success", "message": "Usuarios sincronizados"}), 200
    else:
        usuarios = leer_json("server_usuarios", usuarios_default)
        return jsonify(usuarios), 200

# --- CONTROL DE SESIONES COMPARTIDO ENTRE TODAS LAS INSTANCIAS RENDER ---
# La memoria local de cada proceso no sirve en modo activo-activo: una caja
# puede iniciar sesión en Render A y consultar/cerrar sesión en Render B.
# Por eso el estado de las sesiones se guarda en app_storage de Supabase.
def _leer_sesiones_compartidas():
    sesiones = leer_json("server_sesiones_activas", {})
    return sesiones if isinstance(sesiones, dict) else {}


@app.route('/sesiones/verificar', methods=['GET'])
def verificar_sesion():
    usuario = request.args.get("usuario", "")
    sesiones = _leer_sesiones_compartidas()
    return jsonify({"activo": bool(sesiones.get(usuario, False))}), 200


@app.route('/sesiones/iniciar', methods=['POST'])
def iniciar_sesion():
    data = request.json or {}
    usuario = str(data.get("usuario", "")).strip()
    if not usuario:
        return jsonify({"error": "Usuario no especificado"}), 400

    sesiones = _leer_sesiones_compartidas()
    sesiones[usuario] = True
    if not guardar_json("server_sesiones_activas", sesiones):
        return jsonify({"error": "No se pudo registrar la sesión compartida en Supabase."}), 503
    sesiones_activas[usuario] = True  # Caché local solo informativa.
    return jsonify({"status": "success", "shared": True}), 200


@app.route('/sesiones/cerrar', methods=['POST'])
def cerrar_sesion():
    data = request.json or {}
    usuario = str(data.get("usuario", "")).strip()
    datos_cierre = data.get("datos_cierre", {})
    if not usuario:
        return jsonify({"error": "Usuario no especificado"}), 400

    sesiones = _leer_sesiones_compartidas()
    sesiones[usuario] = False
    sesiones_activas[usuario] = False
    if not guardar_json("server_sesiones_activas", sesiones):
        return jsonify({"error": "No se pudo actualizar el estado de sesión en Supabase."}), 503

    # El administrador no forma parte del cierre consolidado de cajas.
    if "admin" in usuario.lower() or usuario.lower() == "administrador":
        return jsonify({"status": "success", "message": "Sesión de administrador cerrada"}), 200

    cierres_actuales = leer_json("server_cierres_turno", [])
    if not isinstance(cierres_actuales, list):
        cierres_actuales = []
    cierres_actuales = [c for c in cierres_actuales if c.get("usuario") != usuario]
    datos_cierre = dict(datos_cierre) if isinstance(datos_cierre, dict) else {}
    datos_cierre["usuario"] = usuario
    cierres_actuales.append(datos_cierre)
    if not guardar_json("server_cierres_turno", cierres_actuales):
        return jsonify({"error": "No se pudo guardar el cierre de caja en Supabase."}), 503

    cajas_activas_restantes = sum(
        1 for usr, activo in sesiones.items()
        if activo and "admin" not in str(usr).lower()
    )

    if cajas_activas_restantes > 0:
        return jsonify({
            "status": "success",
            "message": f"Cierre de {usuario} en reposo en la nube. Faltan {cajas_activas_restantes} caja(s) por cerrar."
        }), 200

    # Todas las cajas cerraron: enviar reportes individuales y consolidado.
    config = leer_json("server_config", {})
    correo_dest = config.get("correo_destino", "")
    correo_emisor = config.get("correo_emisor", "")
    pass_emisor = config.get("pass_emisor", "")

    if correo_dest and correo_emisor and pass_emisor:
        total_general_pto = 0
        total_general_pago_movil = 0
        total_general_biopago = 0
        total_general_efectivo = 0
        total_general_divisas = 0
        total_general_general = 0
        cuerpo_total_consolidado = "--- RESUMEN CONSOLIDADO GENERAL DE CIERRE DE TODAS LAS CAJAS ---\n\n"

        for cierre in cierres_actuales:
            caja_nombre = cierre.get("usuario", "Caja")
            monto_pto = float(cierre.get("total_punto", cierre.get("punto_venta", 0)))
            monto_pago_movil = float(cierre.get("total_pago_movil", cierre.get("pago_movil", 0)))
            monto_biopago = float(cierre.get("total_biopago", cierre.get("biopago", 0)))
            monto_efectivo = float(cierre.get("total_efectivo", cierre.get("efectivo", 0)))
            monto_divisas = float(cierre.get("total_divisas", cierre.get("divisas", 0)))
            monto_total = float(cierre.get("total_venta", cierre.get("total_general", cierre.get("total_dolares", 0))))

            total_general_pto += monto_pto
            total_general_pago_movil += monto_pago_movil
            total_general_biopago += monto_biopago
            total_general_efectivo += monto_efectivo
            total_general_divisas += monto_divisas
            total_general_general += monto_total

            cuerpo_individual = (
                "Reporte de Cierre de Caja\n"
                f"Cajero / Caja: {caja_nombre}\n"
                f"- Punto de Venta: {monto_pto}\n"
                f"- Pago Móvil: {monto_pago_movil}\n"
                f"- Biopago: {monto_biopago}\n"
                f"- Efectivo: {monto_efectivo}\n"
                f"- Divisas: {monto_divisas}\n"
                f"TOTAL CAJA: {monto_total}\n"
            )
            enviar_correo_smtp(
                correo_dest, correo_emisor, pass_emisor,
                f"Cierre de Turno - {caja_nombre}", cuerpo_individual
            )
            cuerpo_total_consolidado += (
                f"• {caja_nombre} -> Punto: {monto_pto} | Pago Móvil: "
                f"{monto_pago_movil} | Biopago: {monto_biopago} | Total: {monto_total}\n"
            )

        cuerpo_total_consolidado += "\n----------------------------------------\n"
        cuerpo_total_consolidado += f"GRAN TOTAL PUNTO DE VENTA: {total_general_pto}\n"
        cuerpo_total_consolidado += f"GRAN TOTAL PAGO MÓVIL: {total_general_pago_movil}\n"
        cuerpo_total_consolidado += f"GRAN TOTAL BIOPAGO: {total_general_biopago}\n"
        cuerpo_total_consolidado += f"GRAN TOTAL EFECTIVO: {total_general_efectivo}\n"
        cuerpo_total_consolidado += f"GRAN TOTAL DIVISAS: {total_general_divisas}\n"
        cuerpo_total_consolidado += "----------------------------------------\n"
        cuerpo_total_consolidado += f"GRAN TOTAL GENERAL DE LA JORNADA: {total_general_general}\n"
        enviar_correo_smtp(
            correo_dest, correo_emisor, pass_emisor,
            "Consolidado Total de Cierres de Caja", cuerpo_total_consolidado
        )

    if not guardar_json("server_cierres_turno", []):
        return jsonify({"error": "Los correos se procesaron, pero no se pudo limpiar el cierre temporal."}), 503
    if not guardar_json("server_sesiones_activas", {}):
        return jsonify({"error": "Los cierres se procesaron, pero no se pudo limpiar el estado de sesiones."}), 503
    sesiones_activas.clear()

    return jsonify({
        "status": "success",
        "message": "Todas las cajas cerradas. Reportes individuales y consolidado procesados."
    }), 200

# --- RUTAS DE PRODUCTOS ---
def _normalizar_codigo(valor):
    return str(valor or "").strip().casefold()

def _nuevo_id_producto():
    import uuid
    return uuid.uuid4().hex

def _asegurar_ids_productos(productos):
    """
    Garantiza una identidad técnica única sin borrar ningún producto.
    - Los IDs existentes se conservan.
    - Registros sin ID reciben UUID.
    - Si un catálogo histórico contiene el mismo ID en dos productos,
      el primero conserva el ID y a los siguientes se les asigna un UUID nuevo.
    """
    cambio = False
    ids = set()
    for p in productos:
        pid = str(p.get("id", "")).strip() if isinstance(p, dict) else ""
        if not pid or pid in ids:
            nuevo = _nuevo_id_producto()
            while nuevo in ids:
                nuevo = _nuevo_id_producto()
            p["id"] = nuevo
            pid = nuevo
            cambio = True
        ids.add(pid)
    return cambio

def _integridad_catalogo(productos):
    """Audita el catálogo sin borrar ni fusionar registros."""
    errores = []
    ids = {}
    codigos = {}
    invalidos = []
    for indice, p in enumerate(productos or []):
        if not isinstance(p, dict):
            invalidos.append(indice)
            continue
        pid = str(p.get("id", "")).strip()
        codigo = _normalizar_codigo(p.get("codigo"))
        if not pid:
            errores.append({"tipo": "ID_FALTANTE", "indice": indice})
        elif pid in ids:
            errores.append({"tipo": "ID_DUPLICADO", "id": pid, "indices": [ids[pid], indice]})
        else:
            ids[pid] = indice
        if not codigo:
            errores.append({"tipo": "CODIGO_FALTANTE", "indice": indice})
        elif codigo in codigos:
            errores.append({"tipo": "CODIGO_DUPLICADO", "codigo": codigo, "indices": [codigos[codigo], indice]})
        else:
            codigos[codigo] = indice
        try:
            precio = float(p.get("precio", 0) or 0)
            stock = float(p.get("stock", p.get("stock_disp", 0)) or 0)
            if precio < 0 or stock < 0:
                errores.append({"tipo": "VALOR_NEGATIVO", "indice": indice})
        except Exception:
            errores.append({"tipo": "VALOR_INVALIDO", "indice": indice})
    return {"ok": not errores and not invalidos, "errores": errores, "registros_invalidos": invalidos, "cantidad": len(productos or []), "ids_unicos": len(ids), "codigos_unicos": len(codigos)}

def _buscar_por_identidad_segura(productos, producto_id="", codigo=""):
    """ID primero; código solo si no hay ID. Si el código es ambiguo, no elige uno."""
    pid = str(producto_id or "").strip()
    cod = _normalizar_codigo(codigo)
    if pid:
        matches = [(i, p) for i, p in enumerate(productos) if str(p.get("id", "")).strip() == pid]
        if len(matches) == 1:
            return matches[0]
        if len(matches) > 1:
            return "AMBIGUO_ID"
        return None, None
    if cod:
        matches = [(i, p) for i, p in enumerate(productos) if _normalizar_codigo(p.get("codigo")) == cod]
        if len(matches) == 1:
            return matches[0]
        if len(matches) > 1:
            return "AMBIGUO_CODIGO"
    return None, None

def _buscar_producto(productos, producto_id="", codigo=""):
    """Busca por ID primero. El código solo se usa como respaldo cuando no hay ID."""
    pid = str(producto_id or "").strip()
    cod = _normalizar_codigo(codigo)
    if pid:
        for i, p in enumerate(productos):
            if str(p.get("id", "")).strip() == pid:
                return i, p
        return None, None
    if cod:
        for i, p in enumerate(productos):
            if _normalizar_codigo(p.get("codigo")) == cod:
                return i, p
    return None, None

@app.route('/catalogo/version', methods=['GET'])
def obtener_version_catalogo():
    version = obtener_catalogo_version_compartida()
    return jsonify({"catalogo_version": version}), 200

@app.route('/integridad/catalogo', methods=['GET'])
def integridad_catalogo():
    try:
        productos = leer_productos_estricto()
    except Exception as e:
        return jsonify({"ok": False, "error": "No se pudo leer el catálogo.", "detalle": str(e)}), 503
    return jsonify(_integridad_catalogo(productos)), 200

@app.route('/productos', methods=['GET', 'POST'])
def manejar_productos():
    with operaciones_lock:
        try:
            productos = leer_productos_estricto()
        except Exception as e:
            return jsonify({"error": "Inventario no disponible; operación cancelada por seguridad.", "detalle": str(e)}), 503
        productos_antes_ids = [dict(p) for p in productos]
        if _asegurar_ids_productos(productos):
            ok_save, msg_save = guardar_productos_seguro(productos, productos_antes_ids, "normalizacion_ids")
            if not ok_save:
                return jsonify({"error": msg_save}), 500

        if request.method == 'GET':
            respuesta = jsonify(productos)
            respuesta.headers['X-Catalog-Version'] = str(catalogo_version)
            return respuesta, 200

        datos = request.json or {}
        codigo = _normalizar_codigo(datos.get("codigo"))
        prod_id = str(datos.get("id", "")).strip()
        nombre = str(datos.get("nombre", "")).strip()

        if not codigo or not nombre:
            return jsonify({"error": "Código y nombre son obligatorios"}), 400

        indice_id = None
        if prod_id:
            for i, p in enumerate(productos):
                if str(p.get("id", "")).strip() == prod_id:
                    indice_id = i
                    break

        # Un código identifica el producto comercialmente y no puede duplicarse.
        for i, p in enumerate(productos):
            if _normalizar_codigo(p.get("codigo")) == codigo and i != indice_id:
                return jsonify({"error": "Ya existe otro producto con ese código", "producto_existente": p}), 409

        if indice_id is not None:
            productos_antes = [dict(p) for p in productos]
            producto_actual = dict(productos[indice_id])
            producto_actual.update(datos)
            producto_actual["id"] = productos[indice_id].get("id")
            producto_actual.pop("pendiente_sync", None)
            producto_actual.pop("_v8_create", None)
            producto_actual.pop("_v8_edit", None)
            productos[indice_id] = producto_actual
            ok_save, msg_save = guardar_productos_seguro(productos, productos_antes, "editar_producto")
            if not ok_save:
                return jsonify({"error": msg_save}), 500
            return jsonify({"status": "success", "message": "Producto actualizado", "producto": producto_actual, "productos": productos, "catalogo_version": catalogo_version}), 200

        # Si el cliente marca una EDICIÓN y el ID no existe, jamás se cambia de producto
        # silenciosamente usando el código. Esto evita que una identidad rota edite otro artículo.
        if prod_id and datos.get("_v8_edit") and indice_id is None:
            return jsonify({"error": "IDENTIDAD_NO_COINCIDE", "message": "El ID del producto no existe en el catálogo. No se realizó ninguna modificación.", "id": prod_id, "codigo": codigo}), 409

        # Compatibilidad con clientes antiguos: si hay ID nuevo + código nuevo, se crea.
        # Si solo hay código y este ya existe, se actualiza ese producto existente.
        for i, p in enumerate(productos):
            if _normalizar_codigo(p.get("codigo")) == codigo:
                productos_antes = [dict(x) for x in productos]
                producto_actual = dict(p)
                producto_actual.update(datos)
                producto_actual["id"] = p.get("id") or _nuevo_id_producto()
                producto_actual.pop("pendiente_sync", None)
                producto_actual.pop("_v8_create", None)
                producto_actual.pop("_v8_edit", None)
                productos[i] = producto_actual
                ok_save, msg_save = guardar_productos_seguro(productos, productos_antes, "editar_producto_por_codigo")
                if not ok_save:
                    return jsonify({"error": msg_save}), 500
                return jsonify({"status": "success", "message": "Producto actualizado", "producto": producto_actual, "productos": productos, "catalogo_version": catalogo_version}), 200

        nuevo_prod = dict(datos)
        nuevo_prod["id"] = prod_id or _nuevo_id_producto()
        nuevo_prod.pop("pendiente_sync", None)
        nuevo_prod.pop("_v8_create", None)
        nuevo_prod.pop("_v8_edit", None)
        productos.append(nuevo_prod)
        ok_save, msg_save = guardar_productos_seguro(productos, productos[:-1], "crear_producto")
        if not ok_save:
            return jsonify({"error": msg_save}), 500
        return jsonify({"status": "success", "message": "Producto creado", "producto": nuevo_prod, "productos": productos, "catalogo_version": catalogo_version}), 201

@app.route('/productos/<prod_id>', methods=['DELETE'])
def eliminar_producto_servidor(prod_id):
    # PROTECCIÓN PERMANENTE: el POS no permite eliminar productos.
    # Se mantiene la ruta solo por compatibilidad con clientes antiguos,
    # pero jamás modifica server_productos.
    return jsonify({
        "error": "Eliminación de productos desactivada permanentemente para proteger el inventario.",
        "producto_id": str(prod_id),
        "catalogo_modificado": False
    }), 405

# --- RUTAS DE VENTAS Y REVERSIÓN ---
@app.route('/ventas', methods=['GET', 'POST'])
def manejar_ventas():
    if request.method == 'GET':
        return jsonify(leer_json("server_ventas", [])), 200

    nueva_venta = request.json or {}
    operacion_id = str(nueva_venta.get("operacion_id", "")).strip()
    if not operacion_id:
        return jsonify({"error": "Falta operacion_id para garantizar idempotencia"}), 400

    with operaciones_lock:
        ventas = leer_json("server_ventas", [])
        for v in ventas:
            if str(v.get("operacion_id", "")).strip() == operacion_id:
                return jsonify({"status": "success", "id": v.get("id"), "duplicate": True}), 200

        nueva_venta["id"] = max([int(v.get("id", 0)) for v in ventas if str(v.get("id", "")).isdigit()] or [0]) + 1
        nueva_venta["operacion_id"] = operacion_id
        nueva_venta.setdefault("tipo", "Venta")

        lista_items = nueva_venta.get("items", nueva_venta.get("productos", []))
        try:
            productos_server = leer_productos_estricto()
        except Exception as e:
            return jsonify({"error": "Inventario no disponible; venta cancelada por seguridad.", "detalle": str(e)}), 503
        productos_antes = [dict(p) for p in productos_server]
        errores_stock = []

        for item_vendido in lista_items:
            cod_vendido = str(item_vendido.get("codigo", "")).strip().lower()
            id_vendido = str(item_vendido.get("id", "")).strip()
            cant_vendida = float(item_vendido.get("cantidad", item_vendido.get("cant", 1)))
            if cant_vendida <= 0:
                continue

            encontrados = [(i, prod) for i, prod in enumerate(productos_server)
                           if ((id_vendido and str(prod.get("id", "")).strip() == id_vendido)
                               if id_vendido else
                               (cod_vendido and str(prod.get("codigo", "")).strip().casefold() == cod_vendido))]
            if len(encontrados) > 1:
                errores_stock.append({"codigo": cod_vendido, "error": "Identidad ambigua: más de un producto coincide"})
                continue
            if not encontrados:
                errores_stock.append({"codigo": cod_vendido, "error": "Producto no encontrado"})
                continue
            _, prod = encontrados[0]
            stock_actual = float(prod.get("stock", prod.get("stock_disp", 0)))
            if stock_actual < cant_vendida:
                errores_stock.append({"codigo": cod_vendido, "stock": stock_actual, "solicitado": cant_vendida})
            else:
                prod["stock"] = stock_actual - cant_vendida
                if "stock_disp" in prod:
                    prod["stock_disp"] = prod["stock"]

        if errores_stock:
            return jsonify({"error": "Stock insuficiente o producto no encontrado", "detalles": errores_stock}), 409

        ventas.append(nueva_venta)
        ok_save, msg_save = guardar_productos_seguro(productos_server, productos_antes, "venta")
        if not ok_save:
            return jsonify({"error": msg_save}), 500
        if not guardar_json("server_ventas", ventas):
            # Rollback exacto: restaurar el snapshot anterior, no recalcular a mano.
            guardar_json("server_productos", productos_antes)
            return jsonify({"error": "No se pudo guardar la venta; inventario restaurado al estado anterior"}), 500
        return jsonify({"status": "success", "id": nueva_venta["id"], "duplicate": False, "productos": productos_server, "catalogo_version": catalogo_version}), 201


@app.route('/ventas/<int:v_id>', methods=['PUT', 'DELETE'])
def gestionar_venta(v_id):
    """Edición administrativa sin tocar stock y reversión atómica de una venta."""
    with operaciones_lock:
        ventas = leer_json("server_ventas", [])
        venta = next((v for v in ventas if str(v.get("id")) == str(v_id)), None)
        if not venta:
            return jsonify({"error": "Venta no encontrada"}), 404

        if request.method == 'PUT':
            datos = request.json or {}
            anterior = dict(venta)
            # Solo campos administrativos; nunca se permite alterar items/stock desde esta ruta.
            for campo in ("metodo_pago", "detalle", "total_bolivares", "total_dolares"):
                if campo in datos:
                    venta[campo] = datos[campo]
            historial = leer_json("server_ventas_ediciones", [])
            if not isinstance(historial, list): historial = []
            historial.append({"fecha": __import__("datetime").datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "venta_id": v_id, "anterior": anterior, "nuevo": dict(venta)})
            if not guardar_json("server_ventas_ediciones", historial[-500:]):
                return jsonify({"error": "No se pudo guardar el historial de edición; venta no modificada"}), 500
            if not guardar_json("server_ventas", ventas):
                return jsonify({"error": "No se pudo guardar la edición de la venta"}), 500
            return jsonify({"status": "success", "venta": venta, "stock_modificado": False}), 200

        # DELETE aquí significa REVERTIR la venta, no borrar un producto.
        try:
            productos = leer_productos_estricto()
        except Exception as e:
            return jsonify({"error": "Inventario no disponible; reversión cancelada.", "detalle": str(e)}), 503
        productos_antes = [dict(p) for p in productos]
        errores = []
        for item in venta.get("items", venta.get("productos", [])) or []:
            pid = str(item.get("id", "")).strip()
            cod = str(item.get("codigo", "")).strip().casefold()
            cant = float(item.get("cantidad", item.get("cant", 1)) or 0)
            if cant <= 0: continue
            matches = [(i,p) for i,p in enumerate(productos) if ((pid and str(p.get("id", "")).strip()==pid) if pid else (cod and str(p.get("codigo", "")).strip().casefold()==cod))]
            if len(matches) != 1:
                errores.append({"id": pid, "codigo": cod, "error": "Producto no encontrado o identidad ambigua"})
                continue
            prod = matches[0][1]
            prod["stock"] = float(prod.get("stock", prod.get("stock_disp", 0)) or 0) + cant
            if "stock_disp" in prod: prod["stock_disp"] = prod["stock"]
        if errores:
            return jsonify({"error": "No se pudo revertir la venta sin riesgo.", "detalles": errores, "catalogo_modificado": False}), 409
        ok_save, msg_save = guardar_productos_seguro(productos, productos_antes, "revertir_venta")
        if not ok_save:
            return jsonify({"error": msg_save}), 500
        nuevas_ventas = [v for v in ventas if str(v.get("id")) != str(v_id)]
        if not guardar_json("server_ventas", nuevas_ventas):
            guardar_json("server_productos", productos_antes)
            return jsonify({"error": "No se pudo guardar la reversión; inventario restaurado."}), 500
        marcar_catalogo_cambiado()
        return jsonify({"status": "success", "message": "Venta revertida y stock reintegrado.", "productos": productos, "catalogo_version": catalogo_version}), 200

# --- RUTAS DE CLIENTES DE CRÉDITO ---
@app.route('/clientes_credito', methods=['GET', 'POST'])
def manejar_clientes_credito():
    if request.method == 'GET':
        return jsonify(leer_json("server_clientes_credito", [])), 200

    datos = request.json or {}
    nombre = str(datos.get("nombre", "")).strip()
    cedula = str(datos.get("cedula", "")).strip()
    if not nombre or not cedula:
        return jsonify({"error": "Nombre y número de cédula son obligatorios"}), 400

    with operaciones_lock:
        clientes = leer_json("server_clientes_credito", [])
        existente = next((c for c in clientes if str(c.get("cedula", "")).strip() == cedula), None)
        if existente:
            existente.update({"nombre": nombre, "cedula": cedula})
            guardar_json("server_clientes_credito", clientes)
            return jsonify({"status": "success", "cliente": existente, "updated": True}), 200

        nuevo = {
            "id": _nuevo_id_cliente_credito(),
            "nombre": nombre,
            "cedula": cedula,
            "fecha_registro": __import__("datetime").datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        clientes.append(nuevo)
        if not guardar_json("server_clientes_credito", clientes):
            return jsonify({"error": "No se pudo guardar el perfil del cliente"}), 500
        return jsonify({"status": "success", "cliente": nuevo}), 201

def _nuevo_id_cliente_credito():
    import uuid
    return uuid.uuid4().hex

@app.route('/ventas_credito', methods=['GET', 'POST'])
def manejar_ventas_credito():
    if request.method == 'GET':
        return jsonify(leer_json("server_ventas_credito", [])), 200

    datos = request.json or {}
    operacion_id = str(datos.get("operacion_id", "")).strip()
    if not operacion_id:
        return jsonify({"error": "Falta operacion_id para garantizar idempotencia"}), 400

    with operaciones_lock:
        creditos = leer_json("server_ventas_credito", [])
        existente = next((c for c in creditos if str(c.get("operacion_id", "")).strip() == operacion_id), None)
        if existente:
            # Permite actualizar estado/datos del mismo crédito sin volver a tocar inventario.
            existente.update({k: v for k, v in datos.items() if k != "id"})
            guardar_json("server_ventas_credito", creditos)
            return jsonify({"status": "success", "id": existente.get("id"), "duplicate": True}), 200

        # Actualización de un crédito existente por id (ej. marcar como Pagado).
        if datos.get("id") is not None:
            for c in creditos:
                if str(c.get("id")) == str(datos.get("id")):
                    estado_anterior = c.get("estado")
                    c.update(datos)
                    guardar_json("server_ventas_credito", creditos)
                    return jsonify({"status": "success", "id": c.get("id"), "updated": True}), 200

        datos["id"] = max([int(c.get("id", 0)) for c in creditos if str(c.get("id", "")).isdigit()] or [0]) + 1
        datos["operacion_id"] = operacion_id
        datos.setdefault("estado", "Pendiente")

        lista_items = datos.get("items", datos.get("productos", []))
        try:
            productos_server = leer_productos_estricto()
        except Exception as e:
            return jsonify({"error": "Inventario no disponible; crédito cancelado por seguridad.", "detalle": str(e)}), 503
        productos_antes = [dict(p) for p in productos_server]
        errores_stock = []

        for item in lista_items:
            cod = str(item.get("codigo", "")).strip().lower()
            pid = str(item.get("id", "")).strip()
            cant = float(item.get("cantidad", item.get("cant", 1)))
            if cant <= 0:
                continue
            encontrados = [(i, prod) for i, prod in enumerate(productos_server)
                           if ((pid and str(prod.get("id", "")).strip() == pid) if pid else
                               (cod and str(prod.get("codigo", "")).strip().casefold() == cod))]
            if len(encontrados) > 1:
                errores_stock.append({"codigo": cod, "error": "Identidad ambigua: más de un producto coincide"})
                continue
            if not encontrados:
                errores_stock.append({"codigo": cod, "error": "Producto no encontrado"})
                continue
            prod = encontrados[0][1]
            stock = float(prod.get("stock", prod.get("stock_disp", 0)))
            if stock < cant:
                errores_stock.append({"codigo": cod, "stock": stock, "solicitado": cant})
            else:
                prod["stock"] = stock - cant
                if "stock_disp" in prod:
                    prod["stock_disp"] = prod["stock"]

        if errores_stock:
            return jsonify({"error": "Stock insuficiente o producto no encontrado", "detalles": errores_stock}), 409

        creditos.append(datos)
        ok_save, msg_save = guardar_productos_seguro(productos_server, productos_antes, "venta_credito")
        if not ok_save:
            return jsonify({"error": msg_save}), 500
        if not guardar_json("server_ventas_credito", creditos):
            # Rollback exacto usando el snapshot anterior.
            guardar_json("server_productos", productos_antes)
            return jsonify({"error": "No se pudo guardar el crédito; inventario restaurado al estado anterior"}), 500

        marcar_catalogo_cambiado()
        return jsonify({"status": "success", "id": datos["id"], "duplicate": False, "productos": productos_server, "catalogo_version": catalogo_version}), 201

@app.route('/ventas_credito/<int:credito_id>', methods=['DELETE'])
def revertir_venta_credito(credito_id):
    with operaciones_lock:
        creditos = leer_json("server_ventas_credito", [])
        credito = next((c for c in creditos if str(c.get("id")) == str(credito_id)), None)
        if not credito:
            return jsonify({"error": "Crédito ya revertido o no encontrado"}), 404

        items = credito.get("items", credito.get("productos", []))
        try:
            productos = leer_productos_estricto()
        except Exception as e:
            return jsonify({"error": "Inventario no disponible; reversión cancelada por seguridad.", "detalle": str(e)}), 503
        productos_antes = [dict(p) for p in productos]
        for item in items:
            cod = str(item.get("codigo", "")).strip().casefold()
            pid = str(item.get("id", "")).strip()
            cant = float(item.get("cantidad", item.get("cant", 1)) or 0)
            matches = [(i,p) for i,p in enumerate(productos) if ((pid and str(p.get("id", "")).strip()==pid) if pid else (cod and str(p.get("codigo", "")).strip().casefold()==cod))]
            if len(matches) != 1:
                return jsonify({"error": "No se puede revertir el crédito: producto no encontrado o identidad ambigua.", "id": pid, "codigo": cod}), 409
            prod = matches[0][1]
            stock = float(prod.get("stock", prod.get("stock_disp", 0)) or 0)
            prod["stock"] = stock + cant
            if "stock_disp" in prod:
                prod["stock_disp"] = prod["stock"]

        creditos = [c for c in creditos if str(c.get("id")) != str(credito_id)]
        ok_save, msg_save = guardar_productos_seguro(productos, productos_antes, "revertir_credito")
        if not ok_save:
            return jsonify({"error": msg_save}), 500
        if not guardar_json("server_ventas_credito", creditos):
            guardar_json("server_productos", productos_antes)
            return jsonify({"error": "No se pudo guardar el crédito; inventario restaurado al estado anterior."}), 500
        return jsonify({"status": "success", "message": f"Crédito {credito_id} revertido y stock restaurado"}), 200

@app.route('/ventas/reset', methods=['DELETE'])
def reset_ventas():
    guardar_json("server_ventas", [])
    return jsonify({"status": "success", "message": "Historial de ventas reiniciado"}), 200

@app.route('/salud', methods=['GET'])
def salud():
    try:
        productos = leer_productos_estricto()
        integ = _integridad_catalogo(productos)
        return jsonify({"status": "ok", "api_version": SERVER_API_VERSION, "catalogo": integ}), 200
    except Exception as e:
        return jsonify({"status": "error", "api_version": SERVER_API_VERSION, "error": str(e)}), 503

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
