import streamlit as st
from sentence_transformers import SentenceTransformer

def get_embedding_tables(db):
    """Identify tables in LanceDB with at least one embedding/vector column."""
    tables_with_embeddings = []
    for table_name in db.table_names():
        table = db.open_table(table_name)
        # Get schema and look for vector/embedding fields (list/array)
        for field in table.schema:
            # crude check for likely embedding fields (improve as needed)
            if any(
                kw in field.name.lower() for kw in ["embedding", "vector"]
            ) or "list" in str(field.type).lower() or "array" in str(field.type).lower():
                tables_with_embeddings.append((table_name, field.name))
    return tables_with_embeddings

def run_semantic_search_interface(db):
    """Interface for semantic search using vector embeddings."""
    st.subheader("🔍 Semantic Search (Vector Search with Embeddings)")
    
    tables = get_embedding_tables(db)
    if not tables:
        st.info("No tables with detected embedding/vector columns were found.")
        return

    # User select table and embedding column
    table_col_tuples = [f"{t} (column: {c})" for t, c in tables]
    choice = st.selectbox("Select Table & Embedding Column", table_col_tuples)
    sel_table, sel_col = tables[table_col_tuples.index(choice)]

    # Embedding model choices (expand as needed)
    available_models = [
        "all-MiniLM-L6-v2",
        "paraphrase-MiniLM-L6-v2",
        # Add more models installed locally or provide an OpenAI option
    ]
    embed_model = st.selectbox("Embedding Model", available_models)
    
    phrase = st.text_input("Enter your search phrase")
    top_k = st.slider("Number of results", min_value=3, max_value=50, value=10)

    if st.button("Run Semantic Search") and phrase:
        # Use safer encapsulation for SentenceTransformer usage
        try:
            with st.spinner("Vectorizing..."):
                # Create a new embedder for each search to avoid state issues
                embedder = SentenceTransformer(embed_model)
                vec = embedder.encode([phrase], convert_to_numpy=True)[0]  # Ensure numpy array output
            
            with st.spinner("Searching LanceDB..."):
                table = db.open_table(sel_table)
                # Actually run vector query
                ann_df = table.search(vec, vector_column_name=sel_col).limit(top_k).to_pandas()
                st.success(f"Found {len(ann_df)} results (top {top_k})")
                st.dataframe(ann_df, use_container_width=True)
                
        except RuntimeError as e:
            if "no running event loop" in str(e):
                st.error("PyTorch event loop error. Try restarting the Streamlit server.")
                st.code("streamlit run app.py", language="bash")
            elif "Tried to instantiate class" in str(e):
                st.error("PyTorch custom class error. This may be due to incompatibility between PyTorch and Sentence Transformers.")
                st.info("Try downgrading to compatible versions: torch==1.13.1 sentence-transformers==2.2.2")
            else:
                st.error(f"Runtime error: {str(e)}")
        except Exception as e:
            st.error(f"Error performing semantic search: {str(e)}")