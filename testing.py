import streamlit as st

# Inicializar el estado si no está presente
if 'button_clicked' not in st.session_state:
    st.session_state.button_clicked = False


def collect_parameters():
    st.header("Sección de Recolección de Parámetros")
    
    # Recolectar parámetros
    param1 = st.text_input("Parámetro 1")
    param2 = st.number_input("Parámetro 2", min_value=0, max_value=100)
    
    # Botón para indicar que se han terminado de configurar los parámetros
    if 'button_clicked' not in st.session_state:
        st.session_state.button_clicked = False
    if st.session_state.button_clicked:
        st.session_state.param1 = param1
        st.session_state.param2 = param2
        st.session_state.params_collected = True
        st.session_state.button_clicked = False
    else:
        if st.button("Terminar Configuración"):
            st.session_state.button_clicked = True


def submit_params(submit_button):
    if submit_button:
        st.session_state.param1 = 1
        st.session_state.param2 = 3
        st.session_state.params_collected = True


def execute():
    st.header("Sección de Ejecución")
    
    # Usar los parámetros recolectados
    st.write(f"Parámetro 1: {st.session_state.param1}")
    st.write(f"Parámetro 2: {st.session_state.param2}")
    
    # Aquí puedes agregar la lógica de ejecución usando los parámetros
    st.header("Sección de asdad")
    st.write("Ejecutando con los parámetros seleccionados...")

def main():
    st.title("Aplicación en Streamlit")

    if not st.session_state.button_clicked:
        collect_parameters()
    else:
        execute()

if __name__ == "__main__":
    main()