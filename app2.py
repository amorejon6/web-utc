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
def confirm_dialog(prefix):
    st.warning("🔄 Ya existe una evaluación para esta tabla/idioma.")
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
                key=f"download_{prefix}"
            )

        # estado
        if f"{prefix}_col" not in st.session_state:
            st.session_state[f"{prefix}_col"] = 0
        if f"{prefix}_eval" not in st.session_state:
            st.session_state[f"{prefix}_eval"] = {}

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
                    "✅ Correcto",
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
                    "🤏 Conciso",
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

idiomas = sorted(tablas_por_idioma.keys())

# -----------------------------
# SIDEBAR
# -----------------------------
with st.sidebar:
    st.title("📚 Índice")
    counter = 0

    for idioma in idiomas:
        with st.expander(f"🌍 {idioma}"):
            for t in tablas_por_idioma[idioma]:
                st.markdown(f"**{t['id_tabla']}**")

                rdf_content = t["rdf_path"].read_text(encoding="utf-8")
                columnas = parse_dcat_regex(rdf_content)

                prefix = f"{idioma}_{t['id_tabla']}"
                actual = st.session_state.get(f"{prefix}_col", 0)

                for i, col in enumerate(columnas):
                    label = f"#{i+1} {col['nombre'][:15]}"
                    if i == actual:
                        label = f"👉 {label}"

                    if st.button(label, key=f"nav_{counter}"):
                        st.session_state[f"{prefix}_col"] = i
                        st.rerun()

                    counter += 1

    if "username" not in st.session_state:
        st.session_state["username"] = st.text_input("Tu nombre:", "evaluador")

# -----------------------------
# UI PRINCIPAL
# -----------------------------
tabs_idioma = st.tabs(idiomas)

for idioma_tab, idioma in zip(tabs_idioma, idiomas):
    with idioma_tab:
        tablas_idioma = tablas_por_idioma[idioma]

        tabs_tabla = st.tabs([t["id_tabla"] for t in tablas_idioma])

        for tab_tabla, entrada in zip(tabs_tabla, tablas_idioma):
            prefix = f"{idioma}_{entrada['id_tabla']}"

            if f"{prefix}_col" not in st.session_state:
                st.session_state[f"{prefix}_col"] = 0

            render_tab(tab_tabla, entrada, prefix)
