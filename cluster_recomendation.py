import streamlit as st


def main():

    st.title("Polar Bear :polar_bear:")
    st.header('Cluster Recomendator')


    page_names = ['Find Cluster Recommendations', 'Evaluate Cluster Recomendations']

    page = st.radio('Navigation', page_names, index=None)

    if page:
        st.write(f"**Mode {page} **")

    if page == 'Find Cluster Recommendations':

        with st.form(key='base_form', border=False):
            collect_parameters_find_cluster()
            collect_more_parameters_find_cluster()
            submitted = st.form_submit_button("Submit")

        if submitted:
            # execute_parameters_find_cluster(session, df_query, df_tables)
            123

    elif page == 'Evaluate Cluster Recomendations':
        st.button("Do nothing")

    if st.button("Reset"):
        st.session_state.params_collected = False


if __name__ == "__main__":
    main()


