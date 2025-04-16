from airflow import DAG
from datetime import datetime, timedelta
from airflow.decorators import task
from airflow.models import Variable
import pandas as pd
import time
import requests
import os

from sentence_transformers import SentenceTransformer
from pinecone import Pinecone, ServerlessSpec

# Default arguments for DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='Movie_to_Pinecone',
    default_args=default_args,
    description='Build a Movie Posting Search Engine using Pinecone',
    # schedule_interval=timedelta(days=7),
    schedule=timedelta(days=7),
    start_date=datetime(2025, 4, 15),
    catchup=False,
    tags=['movie', 'pinecone', 'search-engine'],
) as dag:
    """
    DAG to build a Amazon movie search engine using Pinecone vector database
    """
    @task
    def download_data():
        """Download Amazon Movie dataset using requests"""
        # Create data directory if it doesn't exist
        data_dir = '/tmp/movie_data'
        os.makedirs(data_dir, exist_ok=True)
        
        # File path to save data
        file_path = f"{data_dir}/movie_data.csv"
        
        # Download the data using requests
        url = 'https://grepp-reco-test.s3.ap-northeast-2.amazonaws.com/tmdb_5000_movies.csv'
        response = requests.get(url)
        
        # Check if the request was successful
        if response.status_code == 200:
            with open(file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            # Count lines to verify
            with open(file_path, 'r') as f:
                line_count = sum(1 for _ in f)
            
            print(f"Downloaded file has {line_count} lines")
        else:
            raise Exception(f"Failed to download data: HTTP Status {response.status_code}")
        
        return file_path
    
    @task
    def preprocess_data(data_path):
        """Clean and prepare data for embedding"""
        # Load data
        df = pd.read_csv(data_path)
        
        # Ensure 'title' exists and fill NA
        df['title'] = df['title'].astype(str).fillna('')

        # You can concatenate with another column like 'overview' if you want more context
        if 'overview' in df.columns:
            df['metadata'] = df.apply(lambda row: {'title': row['title'] + " " + str(row['overview'])}, axis=1)
        else:
            df['metadata'] = df.apply(lambda row: {'title': row['title']}, axis=1)

        # Add ID
        df['id'] = df.reset_index(drop=True).index.astype(str)

        # Save preprocessed data
        preprocessed_path = '/tmp/movie_data/movie_preprocessed.csv'
        df.to_csv(preprocessed_path, index=False)

        print(f"Preprocessed data saved to {preprocessed_path}")
        return preprocessed_path

    
    @task
    def create_pinecone_index():
        """Create or reset Pinecone index"""
        # Get Pinecone API key from Airflow Variables
        api_key = Variable.get("pinecone_api_key")
        
        # Initialize Pinecone
        pc = Pinecone(api_key=api_key)
        
        # Serverless spec for Pinecone
        spec = ServerlessSpec(
            cloud="aws",
            region="us-east-1"
        )
        
        index_name = 'semantic-search-fast'
        
        # Check if index already exists and delete it
        existing_indexes = [index_info["name"] for index_info in pc.list_indexes()]
        if index_name in existing_indexes:
            pc.delete_index(index_name)
        
        # Create new index
        pc.create_index(
            index_name,
            dimension=384,  # dimensionality of minilm
            metric='dotproduct',
            spec=spec
        )
        
        # Wait for index to be initialized
        while not pc.describe_index(index_name).status['ready']:
            time.sleep(1)
        
        print(f"Pinecone index '{index_name}' created successfully")
        return index_name
    
    @task
    def generate_embeddings_and_upsert(data_path, index_name):
        """Generate embeddings and upsert to Pinecone"""
        # Get API key
        api_key = Variable.get("pinecone_api_key")
        
        # Load preprocessed data
        df = pd.read_csv(data_path)
        
        # Initialize embedding model
        model = SentenceTransformer('all-MiniLM-L6-v2', device='cpu')
        
        # Generate embeddings
        print("Generating embeddings...")
        batch_size = 100
        total_batches = (len(df) + batch_size - 1) // batch_size
        
        # Connect to Pinecone
        pc = Pinecone(api_key=api_key)
        index = pc.Index(index_name)
        
        # Process in batches to avoid memory issues
        for i in range(0, len(df), batch_size):
            print(f"Processing batch {i//batch_size + 1}/{total_batches}")
            batch_df = df.iloc[i:i+batch_size].copy()
            
            # Extract metadata entries
            metadata_list = batch_df['metadata'].apply(eval).tolist()
            
            # Generate embeddings for this batch
            titles = [meta['title'] for meta in metadata_list]
            embeddings = model.encode(titles)
            
            # Prepare upsert data
            upsert_data = []
            for j, (_, row) in enumerate(batch_df.iterrows()):
                upsert_data.append({
                    'id': str(row['id']),
                    'values': embeddings[j].tolist(),
                    'metadata': metadata_list[j]
                })
            
            # Upsert to Pinecone
            index.upsert(upsert_data)
        
        print(f"Successfully upserted {len(df)} records to Pinecone")
        return index_name
    
    @task
    def test_search_query(index_name):
        """Test the search with a sample query"""
        # Get API key
        api_key = Variable.get("pinecone_api_key")
        
        # Initialize embedding model
        model = SentenceTransformer('all-MiniLM-L6-v2', device='cpu')
        
        # Connect to Pinecone
        pc = Pinecone(api_key=api_key)
        index = pc.Index(index_name)
        
        # Sample query
        query = "what is ethics in AI"
        query_embedding = model.encode(query).tolist()
        
        # Search
        results = index.query(
            vector=query_embedding,
            top_k=5,
            include_metadata=True
        )
        
        # Print results
        print(f"Search results for query: '{query}'")
        for result in results['matches']:
            print(f"ID: {result['id']}, Score: {result['score']}, Title: {result['metadata']['title'][:50]}...")
    
    # Define task dependencies using the TaskFlow API
    data_path = download_data()
    preprocessed_path = preprocess_data(data_path)
    print(preprocessed_path)
    index_name = create_pinecone_index()
    final_index_name = generate_embeddings_and_upsert(preprocessed_path, index_name)
    test_search_query(final_index_name)
