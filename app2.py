import streamlit as st
import pandas as pd
from pathlib import Path
import re
import csv
from collections import defaultdict
from pymongo import MongoClient
from datetime import datetime

st.set_page_config(page_title="Evaluador Columnas DCAT", layout="wide")

if "evaluador" not in st.session_state:
    st.title("Selecciona tu perfil")
    evaluador = st.selectbox(
        "Elige nombre de usuario de evaluador:",
        options=["evaluador1", "evaluador2", "evaluador3"]
    )
    if st.button("Acceder", type="primary"):
        st.session_state.evaluador = evaluador
        st.rerun()
    st.stop()

BASE_RDFS = Path("rdfs")
BASE_CSVS = Path("csvs")

# Seleccionar evaluador
evaluador_seleccionado = st.session_state.evaluador
st.sidebar.success(f"Evaluador: {evaluador_seleccionado}")
st.session_state.username = evaluador_seleccionado

@st.cache_resource
def init_mongo():
    uri = st.secrets["mongo_uri"]
    return MongoClient(uri)

client = init_mongo()
db = client[st.secrets["db_name"]]
evals_collection = db["evaluaciones"]

def existe_evaluacion_previa(user, tabla, idioma):
    query = {
        "user": user,
        "tabla": tabla,
        "idioma": idioma
    }
    return evals_collection.find_one(query) is not None

def obtener_evaluacion_guardada(user: str, tabla: str, idioma: str):
    query = {"user": user, "tabla": tabla, "idioma": idioma}
    # Por si hubiera duplicados históricos, nos quedamos con la más reciente.
    return evals_collection.find_one(query, sort=[("_id", -1)])

def _normalizar_eval_dict(evaluacion: dict, expected_prefix: str) -> dict:
    """
    Normaliza claves del tipo <prefix>_<col_idx>_<campo> al prefix esperado.
    Esto permite reutilizar evaluaciones guardadas aunque cambie el prefix.
    """
    if not isinstance(evaluacion, dict):
        return {}

    normalizada: dict = {}
    for k, v in evaluacion.items():
        if not isinstance(k, str) or not isinstance(v, dict):
            continue
        try:
            _, col_idx_str, campo = k.rsplit("_", 2)
            int(col_idx_str)  # valida
        except Exception:
            continue
        nk = f"{expected_prefix}_{col_idx_str}_{campo}"
        normalizada[nk] = {
            "correct": bool(v.get("correct", True)),
            "concise": bool(v.get("concise", True)),
            "justif_correct": str(v.get("justif_correct", "") or ""),
            "justif_concise": str(v.get("justif_concise", "") or ""),
        }
    return normalizada

def _eval_resumen_df(doc: dict) -> pd.DataFrame:
    evaluacion = doc.get("evaluacion", {})
    rows = []
    for k, v in (evaluacion or {}).items():
        if not isinstance(k, str) or not isinstance(v, dict):
            continue
        try:
            _, col_idx_str, campo = k.rsplit("_", 2)
            col_num = int(col_idx_str) + 1
        except Exception:
            continue
        rows.append(
            {
                "columna": col_num,
                "campo": campo,
                "correcto": bool(v.get("correct", True)),
                "conciso": bool(v.get("concise", True)),
                "justif_correct": str(v.get("justif_correct", "") or ""),
                "justif_concise": str(v.get("justif_concise", "") or ""),
            }
        )

    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df = df.sort_values(["columna", "campo"]).reset_index(drop=True)
    return df

def _mostrar_evaluacion_guardada(doc: dict, *, etiqueta: str):
    ts = doc.get("timestamp")
    st.info(f"📌 Hay una evaluación guardada ({etiqueta}). Última actualización: {ts or '—'}.")
    df = _eval_resumen_df(doc)
    if df.empty:
        return
    total = len(df)
    incorrectas = int((~df["correcto"]).sum())
    no_concisas = int((~df["conciso"]).sum())
    st.caption(f"Resumen: {total} checks · incorrectas: {incorrectas} · no concisas: {no_concisas}")

def estado_tablas_bilingue(user: str, tablas_ids: list[str]) -> dict[str, bool]:
    """
    Devuelve {id_tabla: True/False} indicando si la tabla está evaluada en ambos idiomas
    (castellano y valenciano) para el usuario.
    """
    if not tablas_ids:
        return {}

    cursor = evals_collection.find(
        {
            "user": user,
            "tabla": {"$in": tablas_ids},
            "idioma": {"$in": ["castellano", "valenciano"]},
        },
        {"tabla": 1, "idioma": 1},
    )

    idiomas_por_tabla: dict[str, set[str]] = defaultdict(set)
    for doc in cursor:
        tabla = str(doc.get("tabla", ""))
        idioma = str(doc.get("idioma", ""))
        if tabla and idioma:
            idiomas_por_tabla[tabla].add(idioma)

    return {tid: idiomas_por_tabla.get(tid, set()) >= {"castellano", "valenciano"} for tid in tablas_ids}

def validar_justificaciones(evaluacion_dict):
    faltantes = []
    for eval_key, data in evaluacion_dict.items():
        try:
            _, col_idx_str, campo = eval_key.rsplit("_", 2)
            col_num = int(col_idx_str) + 1
            base = f"Columna {col_num} - {campo}"
        except Exception:
            base = str(eval_key)

        if data.get("correct") is False and not str(data.get("justif_correct", "")).strip():
            faltantes.append(f"{base}: falta la justificacion de por que NO es correcto.")
        if data.get("concise") is False and not str(data.get("justif_concise", "")).strip():
            faltantes.append(f"{base}: falta la justificacion de por que NO es conciso.")

    return faltantes

# -----------------------------
# Utilidades
# -----------------------------

@st.dialog("Confirmar actualización", width="medium")
def confirm_dialog(prefix, message=None):
    st.warning(message or "🔄 Ya existe una evaluación para esta tabla/idioma.")
    st.info("Se sobrescribirá la anterior. ¿Continuar?")
    
    col1, col2 = st.columns([2, 2])
    with col1:
        if st.button("❌ Cancelar", type="secondary", key=f"{prefix}_dialog_cancel"):
            st.session_state[f"confirmar_{prefix}"] = False
            st.rerun()
    with col2:
        if st.button("✅ Actualizar", type="primary", key=f"{prefix}_dialog_ok"):
            st.session_state[f"confirmar_{prefix}"] = True
            st.rerun()

def find_delimiter(csv_file, delimiters=";,\\t|", delimiter_default=";"):
    try:
        with open(csv_file, "r", encoding="utf-8", newline="") as f:
            sample = f.readline()
        return csv.Sniffer().sniff(sample, delimiters=delimiters).delimiter
    except:
        pass

    try:
        with open(csv_file, "r", encoding="utf-8", newline="") as f:
            line = f.readline()
        counts = [(line.count(d), d) for d in delimiters]
        return max(counts)[1]
    except:
        return delimiter_default

def descubrir_tablas():
    tablas = []
    if not BASE_RDFS.exists():
        return tablas

    for rdf_idioma_dir in BASE_RDFS.iterdir():
        if not rdf_idioma_dir.is_dir():
            continue

        idioma = rdf_idioma_dir.name
        csv_idioma_dir = BASE_CSVS / idioma

        for rdf_path in rdf_idioma_dir.glob("*.rdf"):
            id_tabla = rdf_path.stem
            csv_path = csv_idioma_dir / f"{id_tabla}.csv"

            tablas.append({
                "idioma": idioma,
                "id_tabla": id_tabla,
                "rdf_path": rdf_path,
                "csv_path": csv_path if csv_path.exists() else None,
            })

    return sorted(tablas, key=lambda x: (x["idioma"], x["id_tabla"]))

# -----------------------------
# Parser RDF
# -----------------------------
def parse_dcat_regex(rdf_content):
    nombres = re.findall(r'<schema:name[^>]*>([^<]+)</schema:name>', rdf_content, re.IGNORECASE)
    descripciones = re.findall(r'<schema:description[^>]*>([^<]+?)</schema:description>', rdf_content, re.IGNORECASE)
    tipos_raw = re.findall(r'schema:valueType[^>]*rdf:resource=[\"|\']([^\"|\']+)["|\']', rdf_content, re.IGNORECASE)

    tipo_map = {
        "http://www.w3.org/2001/XMLSchema#string": "string",
        "http://www.w3.org/2001/XMLSchema#integer": "integer",
        "http://www.w3.org/2001/XMLSchema#double": "double",
        "http://www.w3.org/2001/XMLSchema#decimal": "decimal",
    }

    # print(f"DEBUG: Encontrados {len(nombres)} nombres, {len(descripciones)} descripciones, {len(tipos_raw)} tipos")
    max_cols = max(len(nombres), len(descripciones), len(tipos_raw))

    columnas = []
    for i in range(max_cols):
        columnas.append({
            "numero": i + 1,
            "nombre": nombres[i] if i < len(nombres) else f"Col_{i+1}",
            "descripcion": descripciones[i] if i < len(descripciones) else "",
            "tipo": tipo_map.get(tipos_raw[i], "string") if i < len(tipos_raw) else "string",
        })

    return columnas

# -----------------------------
# UI tabla con evaluación
# -----------------------------
def render_tab(tab, entrada, prefix):
    idioma = entrada["idioma"]
    id_tabla = entrada["id_tabla"]
    rdf_path = entrada["rdf_path"]
    csv_path = entrada["csv_path"]

    with tab:
        user = st.session_state.evaluador
        rdf_content = rdf_path.read_text(encoding="utf-8")
        COLUMNAS = parse_dcat_regex(rdf_content)

        total_columnas = len(COLUMNAS)

        st.title(f"{id_tabla} ({idioma})")

        # CSV
        if csv_path and csv_path.exists():
            sep = find_delimiter(csv_path)
            df_csv = pd.read_csv(csv_path, sep=sep)
            st.download_button(
                "📥 Descargar CSV",
                df_csv.to_csv(index=False).encode("utf-8"),
                file_name=csv_path.name,
                key=f"download_{prefix}_mono"
            )

        # estado
        if f"{prefix}_col" not in st.session_state:
            st.session_state[f"{prefix}_col"] = 0
        if f"{prefix}_eval" not in st.session_state:
            st.session_state[f"{prefix}_eval"] = {}

        # Si existe evaluación guardada, la mostramos y (si procede) precargamos el formulario
        saved_doc = obtener_evaluacion_guardada(user, id_tabla, idioma)
        if saved_doc:
            _mostrar_evaluacion_guardada(saved_doc, etiqueta=f"{id_tabla} · {idioma}")
            if not st.session_state[f"{prefix}_eval"]:
                st.session_state[f"{prefix}_eval"] = _normalizar_eval_dict(saved_doc.get("evaluacion", {}), prefix)
                st.caption("✅ Evaluación guardada precargada en el formulario.")

        col_idx = st.session_state[f"{prefix}_col"]
        col_data = COLUMNAS[col_idx]

        st.subheader(f"Columna {col_data['numero']}: {col_data['nombre']}")

        st.table(pd.DataFrame([col_data]))

        # -----------------------------
        # Evaluación
        # -----------------------------
        def evaluar(campo, valor):
            key = f"{prefix}_{col_idx}_{campo}"

            if key not in st.session_state[f"{prefix}_eval"]:
                st.session_state[f"{prefix}_eval"][key] = {
                    "correct": True,
                    "concise": True,
                    "justif_correct": "",
                    "justif_concise": ""
                }

            data = st.session_state[f"{prefix}_eval"][key]

            st.markdown(f"### {campo.upper()}: {valor}")

            c1, c2 = st.columns(2)

            with c1:
                data["correct"] = st.checkbox(
                    "Correcto",
                    value=data["correct"],
                    key=f"{key}_c"
                )
                if not data["correct"]:
                    data["justif_correct"] = st.text_area(
                        "Justificación",
                        value=data["justif_correct"],
                        key=f"{key}_jc"
                    )

            with c2:
                data["concise"] = st.checkbox(
                    "Conciso",
                    value=data["concise"],
                    key=f"{key}_cc"
                )
                if not data["concise"]:
                    data["justif_concise"] = st.text_area(
                        "Justificación",
                        value=data["justif_concise"],
                        key=f"{key}_jcc"
                    )

            st.session_state[f"{prefix}_eval"][key] = data

        evaluar("nombre", col_data["nombre"])
        evaluar("descripcion", col_data["descripcion"])
        evaluar("tipo", col_data["tipo"])

        # navegación
        col1, col2 = st.columns(2)

        with col1:
            if st.button("◀️", disabled=col_idx == 0, key=f"{prefix}_prev"):
                st.session_state[f"{prefix}_col"] = max(0, st.session_state[f"{prefix}_col"] - 1)
                st.rerun()

        with col2:
            if st.button("▶️", disabled=col_idx == total_columnas - 1, key=f"{prefix}_next"):
                st.session_state[f"{prefix}_col"] = min(total_columnas - 1, st.session_state[f"{prefix}_col"] + 1)
                st.rerun()

        if col_idx == total_columnas - 1:
            st.markdown("---")

            # Inicializar estado persistente
            key_previa = f"{prefix}_tiene_previa"
            if key_previa not in st.session_state:
                st.session_state[key_previa] = existe_evaluacion_previa(st.session_state.evaluador, id_tabla, idioma)
            tiene_previa = st.session_state[key_previa]
            
            user = st.session_state.evaluador
            tabla = id_tabla
            idioma_actual = idioma
            tiene_previa = existe_evaluacion_previa(user, tabla, idioma_actual)
            
            evaluacion = st.session_state[f"{prefix}_eval"].copy()
            doc = {
                "tabla": tabla,
                "idioma": idioma_actual,
                "columnas": COLUMNAS,
                "evaluacion": evaluacion,
                "timestamp": datetime.now().isoformat(),
                "user": user
            }

            boton_texto = "🔄 Actualizar Evaluación" if tiene_previa else "💾 Guardar Evaluación"

            # Mensaje/animacion tras guardar o actualizar (se muestra en el rerun siguiente)
            flash_key = f"{prefix}_flash"
            flash = st.session_state.get(flash_key)
            if flash == "saved":
                st.success("¡Evaluacion guardada exitosamente!")
                st.balloons()
                st.session_state[flash_key] = None
            elif flash == "updated":
                st.success("¡Evaluacion actualizada!")
                st.balloons()
                st.session_state[flash_key] = None

            confirm_key = f"confirmar_{prefix}"
            if confirm_key not in st.session_state:
                st.session_state[confirm_key] = None

            # Si el usuario ya confirmo/cancelo en el dialogo, procesamos aqui (fuera del click del boton principal)
            if st.session_state[confirm_key] is True:
                faltantes = validar_justificaciones(evaluacion)
                if faltantes:
                    st.error("Faltan justificaciones obligatorias. Completa los campos y vuelve a intentar.")
                    st.write(faltantes)
                    st.session_state[confirm_key] = None
                    st.stop()

                query = {"user": user, "tabla": tabla, "idioma": idioma_actual}
                update_result = evals_collection.update_one(query, {"$set": doc}, upsert=True)
                st.session_state[confirm_key] = None
                st.session_state[key_previa] = True
                st.session_state[flash_key] = "updated"
                st.rerun()

            elif st.session_state[confirm_key] is False:
                st.info("Actualizacion cancelada.")
                st.session_state[confirm_key] = None

            if st.button(boton_texto, type="primary", key=f"{prefix}_enviar"):
                faltantes = validar_justificaciones(evaluacion)
                if faltantes:
                    st.error("Faltan justificaciones obligatorias. Completa los campos y vuelve a intentar.")
                    st.write(faltantes)
                    st.stop()

                if tiene_previa:
                    confirm_dialog(prefix)
                else:
                    result = evals_collection.insert_one(doc)
                    if result.acknowledged and result.inserted_id:
                        st.session_state[key_previa] = True
                        st.session_state[flash_key] = "saved"
                        st.rerun()
                    else:
                        st.error("Error al guardar nueva evaluacion.")

def _ensure_eval_entry(storage_prefix: str, col_idx: int, campo: str):
    if f"{storage_prefix}_eval" not in st.session_state:
        st.session_state[f"{storage_prefix}_eval"] = {}

    storage_key = f"{storage_prefix}_{col_idx}_{campo}"
    if storage_key not in st.session_state[f"{storage_prefix}_eval"]:
        st.session_state[f"{storage_prefix}_eval"][storage_key] = {
            "correct": True,
            "concise": True,
            "justif_correct": "",
            "justif_concise": "",
        }
    return storage_key, st.session_state[f"{storage_prefix}_eval"][storage_key]

def _render_eval_field(storage_prefix: str, ui_prefix: str, col_idx: int, campo: str, valor: str, *, disabled: bool = False):
    storage_key, data = _ensure_eval_entry(storage_prefix, col_idx, campo)
    ui_key = f"{ui_prefix}_{col_idx}_{campo}"

    st.markdown(f"### {campo.upper()}: {valor if str(valor).strip() else '—'}")
    c1, c2 = st.columns(2)

    with c1:
        data["correct"] = st.checkbox(
            "Correcto",
            value=data["correct"],
            key=f"{ui_key}_c",
            disabled=disabled,
        )
        if not data["correct"]:
            data["justif_correct"] = st.text_area(
                "Justificación",
                value=data["justif_correct"],
                key=f"{ui_key}_jc",
                disabled=disabled,
            )

    with c2:
        data["concise"] = st.checkbox(
            "Conciso",
            value=data["concise"],
            key=f"{ui_key}_cc",
            disabled=disabled,
        )
        if not data["concise"]:
            data["justif_concise"] = st.text_area(
                "Justificación",
                value=data["justif_concise"],
                key=f"{ui_key}_jcc",
                disabled=disabled,
            )

    st.session_state[f"{storage_prefix}_eval"][storage_key] = data

def render_tab_bilingue(tab, entrada_es, entrada_va, id_tabla):
    prefix_es = f"castellano_{id_tabla}"
    prefix_va = f"valenciano_{id_tabla}"
    prefix_bi = f"bilingue_{id_tabla}"
    ui_es = f"{prefix_bi}_ui_es"
    ui_va = f"{prefix_bi}_ui_va"

    with tab:
        user = st.session_state.evaluador
        rdf_content_es = entrada_es["rdf_path"].read_text(encoding="utf-8")
        rdf_content_va = entrada_va["rdf_path"].read_text(encoding="utf-8")
        columnas_es = parse_dcat_regex(rdf_content_es)
        columnas_va = parse_dcat_regex(rdf_content_va)

        total_columnas = max(len(columnas_es), len(columnas_va))

        st.title(f"{id_tabla} (castellano + valenciano)")

        # CSVs
        csv_path_es = entrada_es.get("csv_path")
        csv_path_va = entrada_va.get("csv_path")
        dl1, dl2 = st.columns(2)
        with dl1:
            if csv_path_es and csv_path_es.exists():
                sep = find_delimiter(csv_path_es)
                df_csv = pd.read_csv(csv_path_es, sep=sep)
                st.download_button(
                    "📥 Descargar CSV (castellano)",
                    df_csv.to_csv(index=False).encode("utf-8"),
                    file_name=csv_path_es.name,
                    key=f"download_{prefix_es}_bi",
                )
        with dl2:
            if csv_path_va and csv_path_va.exists():
                sep = find_delimiter(csv_path_va)
                df_csv = pd.read_csv(csv_path_va, sep=sep)
                st.download_button(
                    "📥 Descargar CSV (valenciano)",
                    df_csv.to_csv(index=False).encode("utf-8"),
                    file_name=csv_path_va.name,
                    key=f"download_{prefix_va}_bi",
                )

        # estado
        if f"{prefix_bi}_col" not in st.session_state:
            st.session_state[f"{prefix_bi}_col"] = 0
        if f"{prefix_es}_eval" not in st.session_state:
            st.session_state[f"{prefix_es}_eval"] = {}
        if f"{prefix_va}_eval" not in st.session_state:
            st.session_state[f"{prefix_va}_eval"] = {}

        # Si existen evaluaciones guardadas, las mostramos y precargamos el formulario si está vacío
        saved_es = obtener_evaluacion_guardada(user, id_tabla, "castellano")
        saved_va = obtener_evaluacion_guardada(user, id_tabla, "valenciano")
        if saved_es:
            _mostrar_evaluacion_guardada(saved_es, etiqueta=f"{id_tabla} · castellano")
        if saved_va:
            _mostrar_evaluacion_guardada(saved_va, etiqueta=f"{id_tabla} · valenciano")

        if saved_es and not st.session_state[f"{prefix_es}_eval"]:
            st.session_state[f"{prefix_es}_eval"] = _normalizar_eval_dict(saved_es.get("evaluacion", {}), prefix_es)
        if saved_va and not st.session_state[f"{prefix_va}_eval"]:
            st.session_state[f"{prefix_va}_eval"] = _normalizar_eval_dict(saved_va.get("evaluacion", {}), prefix_va)

        if (saved_es or saved_va) and (st.session_state[f"{prefix_es}_eval"] or st.session_state[f"{prefix_va}_eval"]):
            st.caption("✅ Evaluación guardada precargada en el formulario (si estaba vacío).")

        col_idx = st.session_state[f"{prefix_bi}_col"]

        col_data_es = columnas_es[col_idx] if col_idx < len(columnas_es) else None
        col_data_va = columnas_va[col_idx] if col_idx < len(columnas_va) else None

        st.subheader(f"Columna {col_idx + 1}")

        meta1, meta2 = st.columns(2)
        with meta1:
            st.caption("Castellano")
            if col_data_es:
                st.table(pd.DataFrame([col_data_es]))
            else:
                st.info("No existe esta columna en castellano.")
        with meta2:
            st.caption("Valenciano")
            if col_data_va:
                st.table(pd.DataFrame([col_data_va]))
            else:
                st.info("No existe esta columna en valenciano.")

        st.markdown("---")

        def render_campo(campo: str, valor_es: str, valor_va: str):
            st.markdown(f"## {campo.upper()}")
            c_es, c_va = st.columns(2)

            with c_es:
                _render_eval_field(prefix_es, ui_es, col_idx, campo, valor_es, disabled=col_data_es is None)

            with c_va:
                _render_eval_field(prefix_va, ui_va, col_idx, campo, valor_va, disabled=col_data_va is None)

        render_campo(
            "nombre",
            col_data_es["nombre"] if col_data_es else "",
            col_data_va["nombre"] if col_data_va else "",
        )
        render_campo(
            "descripcion",
            col_data_es["descripcion"] if col_data_es else "",
            col_data_va["descripcion"] if col_data_va else "",
        )
        render_campo(
            "tipo",
            col_data_es["tipo"] if col_data_es else "",
            col_data_va["tipo"] if col_data_va else "",
        )

        # navegación (abajo, con "siguiente" alineado a la derecha)
        nav_left, nav_mid, nav_right = st.columns([1, 6, 1])
        with nav_left:
            if st.button("◀️ Anterior", disabled=col_idx == 0, key=f"{prefix_bi}_prev"):
                st.session_state[f"{prefix_bi}_col"] = max(0, st.session_state[f"{prefix_bi}_col"] - 1)
                st.rerun()
        with nav_right:
            if st.button("Siguiente ▶️", disabled=col_idx == total_columnas - 1, key=f"{prefix_bi}_next"):
                st.session_state[f"{prefix_bi}_col"] = min(total_columnas - 1, st.session_state[f"{prefix_bi}_col"] + 1)
                st.rerun()

        if col_idx == total_columnas - 1:
            st.markdown("---")

            user = st.session_state.evaluador
            tabla = id_tabla

            tiene_previa_es = existe_evaluacion_previa(user, tabla, "castellano")
            tiene_previa_va = existe_evaluacion_previa(user, tabla, "valenciano")
            tiene_previa = tiene_previa_es or tiene_previa_va

            eval_es = st.session_state[f"{prefix_es}_eval"].copy()
            eval_va = st.session_state[f"{prefix_va}_eval"].copy()

            doc_es = {
                "tabla": tabla,
                "idioma": "castellano",
                "columnas": columnas_es,
                "evaluacion": eval_es,
                "timestamp": datetime.now().isoformat(),
                "user": user,
            }
            doc_va = {
                "tabla": tabla,
                "idioma": "valenciano",
                "columnas": columnas_va,
                "evaluacion": eval_va,
                "timestamp": datetime.now().isoformat(),
                "user": user,
            }

            boton_texto = "🔄 Actualizar evaluación de ambos idiomas" if tiene_previa else "💾 Guardar evaluación de ambos idiomas"

            flash_key = f"{prefix_bi}_flash"
            flash = st.session_state.get(flash_key)
            if flash == "saved":
                st.success("¡Evaluaciones guardadas!")
                st.balloons()
                st.session_state[flash_key] = None
            elif flash == "updated":
                st.success("¡Evaluaciones actualizadas!")
                st.balloons()
                st.session_state[flash_key] = None

            confirm_key = f"confirmar_{prefix_bi}"
            if confirm_key not in st.session_state:
                st.session_state[confirm_key] = None

            if st.session_state[confirm_key] is True:
                faltantes = validar_justificaciones(eval_es) + validar_justificaciones(eval_va)
                if faltantes:
                    st.error("Faltan justificaciones obligatorias. Completa los campos y vuelve a intentar.")
                    st.write(faltantes)
                    st.session_state[confirm_key] = None
                    st.stop()

                query_es = {"user": user, "tabla": tabla, "idioma": "castellano"}
                query_va = {"user": user, "tabla": tabla, "idioma": "valenciano"}
                evals_collection.update_one(query_es, {"$set": doc_es}, upsert=True)
                evals_collection.update_one(query_va, {"$set": doc_va}, upsert=True)
                st.session_state[confirm_key] = None
                st.session_state[flash_key] = "updated" if tiene_previa else "saved"
                st.rerun()
            elif st.session_state[confirm_key] is False:
                st.info("Actualizacion cancelada.")
                st.session_state[confirm_key] = None

            if st.button(boton_texto, type="primary", key=f"{prefix_bi}_enviar"):
                faltantes = validar_justificaciones(eval_es) + validar_justificaciones(eval_va)
                if faltantes:
                    st.error("Faltan justificaciones obligatorias. Completa los campos y vuelve a intentar.")
                    st.write(faltantes)
                    st.stop()

                if tiene_previa:
                    confirm_dialog(prefix_bi, message="🔄 Ya existe una evaluación para esta tabla en castellano y/o valenciano.")
                else:
                    query_es = {"user": user, "tabla": tabla, "idioma": "castellano"}
                    query_va = {"user": user, "tabla": tabla, "idioma": "valenciano"}
                    evals_collection.update_one(query_es, {"$set": doc_es}, upsert=True)
                    evals_collection.update_one(query_va, {"$set": doc_va}, upsert=True)
                    st.session_state[flash_key] = "saved"
                    st.rerun()

# -----------------------------
# MAIN
# -----------------------------
tablas = descubrir_tablas()

if not tablas:
    st.warning("No hay datos en rdfs/ o csvs/")
    st.stop()

tablas_por_idioma = defaultdict(list)
for t in tablas:
    tablas_por_idioma[t["idioma"]].append(t)

# tablas bilingües (castellano + valenciano)
bilingue_tablas = {}
if "castellano" in tablas_por_idioma and "valenciano" in tablas_por_idioma:
    es_map = {t["id_tabla"]: t for t in tablas_por_idioma["castellano"]}
    va_map = {t["id_tabla"]: t for t in tablas_por_idioma["valenciano"]}
    for id_tabla in sorted(set(es_map.keys()) & set(va_map.keys())):
        bilingue_tablas[id_tabla] = (es_map[id_tabla], va_map[id_tabla])

if not bilingue_tablas:
    st.warning("No hay tablas bilingües (castellano + valenciano). Revisa la carpeta rdfs/ y csvs/.")
    st.stop()

if "bilingue_tabla_activa" not in st.session_state:
    st.session_state["bilingue_tabla_activa"] = next(iter(bilingue_tablas.keys()))

# -----------------------------
# SIDEBAR
# -----------------------------
with st.sidebar:
    st.title("📚 Índice")
    counter = 0
    ids = list(bilingue_tablas.keys())
    estado = estado_tablas_bilingue(st.session_state.evaluador, ids)
    total_evaluadas = sum(1 for tid in ids if estado.get(tid))
    st.caption(f"Evaluadas: {total_evaluadas}/{len(ids)}")

    with st.expander("🧩 tablas a evaluar"):
        for id_tabla, (t_es, _) in bilingue_tablas.items():
            badge = "✅ (evaluada)" if estado.get(id_tabla) else "⏳ (no evaluada)"
            st.markdown(f"**{id_tabla}** {badge}")
            rdf_content = t_es["rdf_path"].read_text(encoding="utf-8")
            columnas = parse_dcat_regex(rdf_content)

            prefix_bi = f"bilingue_{id_tabla}"
            actual = st.session_state.get(f"{prefix_bi}_col", 0)
            tabla_activa = st.session_state.get("bilingue_tabla_activa")
            es_tabla_activa = tabla_activa == id_tabla

            for i, col in enumerate(columnas):
                label = f"#{i+1} {col['nombre'][:15]}"
                if es_tabla_activa and i == actual:
                    label = f"👉 {label}"

                if st.button(label, key=f"nav_bi_{counter}"):
                    st.session_state["bilingue_tabla_activa"] = id_tabla
                    st.session_state[f"{prefix_bi}_col"] = i
                    st.rerun()

                counter += 1

# -----------------------------
# UI PRINCIPAL
# -----------------------------
tabs_main = st.tabs(["Tablas a evaluar"])
with tabs_main[0]:
    def _fmt_tabla(tid: str) -> str:
        return f"{tid} {'✅ (evaluada)' if estado.get(tid) else '⏳ (no evaluada)'}"

    id_tabla = st.selectbox("Tabla", ids, key="bilingue_tabla_activa", format_func=_fmt_tabla)
    entrada_es, entrada_va = bilingue_tablas[id_tabla]
    render_tab_bilingue(st.container(), entrada_es, entrada_va, id_tabla)
