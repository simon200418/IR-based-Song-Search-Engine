import os
import csv
import pandas as pd
from whoosh.fields import Schema, TEXT, ID, NUMERIC
from whoosh.analysis import StemmingAnalyzer, StandardAnalyzer
from whoosh import index
from whoosh.qparser import MultifieldParser, QueryParser
from whoosh.query import And, Or
import streamlit as st
import shutil
from datetime import datetime
import re

class SongSearchEngine:
    def __init__(self, csv_file="song.csv", index_dir="indexdir"):
        self.csv_file = csv_file
        self.index_dir = index_dir
        self.ix = None
        self.songs_data = []
        
        # Initialize the search engine
        self.setup_schema()
        self.load_data()
        self.build_index()
    
    def normalize(self, text):
        """Normalize text for consistent searching"""
        if not text:
            return ""
        # Remove extra whitespace and convert to lowercase
        text = re.sub(r'\s+', ' ', str(text).strip().lower())
        return text
    
    def setup_schema(self):
        """Define the search index schema"""
        # Use stemming analyzer but keep important words
        analyzer = StemmingAnalyzer(stoplist=frozenset(['and', 'or', 'but']))
        
        self.schema = Schema(
            song_id=ID(stored=True, unique=True),
            title=TEXT(stored=True, analyzer=analyzer, phrase=True),
            artist=TEXT(stored=True, analyzer=analyzer, phrase=True),
            lyrics=TEXT(stored=True, analyzer=analyzer),
            title_exact=TEXT(stored=True, analyzer=StandardAnalyzer()),
            artist_exact=TEXT(stored=True, analyzer=StandardAnalyzer())
        )
    
    def load_data(self):
        """Load songs from CSV file"""
        try:
            if os.path.exists(self.csv_file):
                df = pd.read_csv(self.csv_file)
                # Ensure all required columns exist
                required_columns = ['song_id', 'title', 'artist', 'lyrics']
                missing_columns = [col for col in required_columns if col not in df.columns]
                
                if missing_columns:
                    st.error(f"Missing required columns in CSV: {missing_columns}")
                    st.info("Required columns: song_id, title, artist, lyrics")
                    self.songs_data = []
                    return
                
                self.songs_data = df.to_dict('records')
                st.success(f"Loaded {len(self.songs_data)} songs from {self.csv_file}")
            else:
                st.error(f"CSV file '{self.csv_file}' not found!")
                st.info("Please ensure your CSV file is in the same directory as this app.")
                st.info("Expected format: song_id,title,artist,lyrics")
                self.songs_data = []
        except Exception as e:
            st.error(f"Error loading data: {e}")
            self.songs_data = []
    
    def build_index(self):
        """Build the search index"""
        if not self.songs_data:
            st.error("No data available to build index. Please check your CSV file.")
            return
            
        try:
            # Remove existing index with proper error handling
            if os.path.exists(self.index_dir):
                try:
                    shutil.rmtree(self.index_dir)
                except PermissionError:
                    # If we can't delete, try to work with existing index
                    st.warning(f"Could not delete existing index directory. Attempting to overwrite...")
                    pass
            os.makedirs(self.index_dir, exist_ok=True)
            
            # Create new index
            self.ix = index.create_in(self.index_dir, self.schema)
            writer = self.ix.writer()
            
            # Add documents to index
            for song in self.songs_data:
                writer.add_document(
                    song_id=str(song.get("song_id", "")),
                    title=self.normalize(song.get("title", "")),
                    artist=self.normalize(song.get("artist", "")),
                    lyrics=self.normalize(song.get("lyrics", "")),
                    title_exact=str(song.get("title", "")),
                    artist_exact=str(song.get("artist", ""))
                )
            
            writer.commit()
            st.success("Search index built successfully!")
            
        except Exception as e:
            st.error(f"Error building index: {e}")
    
    def search_songs(self, query_str, search_fields=None, top_n=10):
        """Search songs using the query string"""
        if not self.ix or not query_str.strip():
            return []
        
        try:
            with self.ix.searcher() as searcher:
                # Default search fields
                if not search_fields:
                    search_fields = ["title", "artist", "lyrics"]
                
                # Create parser for multiple fields
                parser = MultifieldParser(search_fields, schema=self.ix.schema)
                
                # Parse and execute query
                query = parser.parse(self.normalize(query_str))
                results = searcher.search(query, limit=top_n)
                
                # Extract results with scores
                search_results = []
                for r in results:
                    search_results.append({
                        'song_id': r["song_id"],
                        'title': r["title_exact"] if "title_exact" in r else r["title"],
                        'artist': r["artist_exact"] if "artist_exact" in r else r["artist"],
                        'score': r.score
                    })
                
                return search_results
                
        except Exception as e:
            st.error(f"Search error: {e}")
            return []
    
    def get_song_details(self, song_id):
        """Get full details for a specific song"""
        for song in self.songs_data:
            if str(song.get("song_id")) == str(song_id):
                return song
        return None
    
    def get_search_suggestions(self, query_str, max_suggestions=5):
        """Get search suggestions based on partial query"""
        if not query_str or len(query_str) < 2:
            return []
        
        suggestions = set()
        query_lower = query_str.lower()
        
        for song in self.songs_data:
            # Check title matches
            title = song.get("title", "").lower()
            if query_lower in title:
                suggestions.add(song.get("title", ""))
            
            # Check artist matches  
            artist = song.get("artist", "").lower()
            if query_lower in artist:
                suggestions.add(song.get("artist", ""))
        
        return list(suggestions)[:max_suggestions]

def main():
    st.set_page_config(
        page_title="Song Search Engine",
        page_icon="🎵",
        layout="wide"
    )
    
    st.title("🎵 Advanced Song Search Engine")
    st.markdown("*An Information Retrieval System for Music Discovery*")
    
    # Initialize search engine
    if 'search_engine' not in st.session_state:
        with st.spinner("Initializing search engine..."):
            st.session_state.search_engine = SongSearchEngine()
    
    search_engine = st.session_state.search_engine
    
    # Sidebar for advanced options
    with st.sidebar:
        st.header("Search Options")
        
        # Search field selection
        st.subheader("Search In:")
        search_title = st.checkbox("Title", value=True)
        search_artist = st.checkbox("Artist", value=True) 
        search_lyrics = st.checkbox("Lyrics", value=True)
        
        # Number of results
        max_results = st.slider("Max Results", min_value=1, max_value=50, value=10)
        
        st.markdown("---")
        st.subheader("Dataset Info")
        st.info(f"Total Songs: {len(search_engine.songs_data)}")
        
        # Rebuild index button
        if st.button("🔄 Rebuild Index"):
            with st.spinner("Rebuilding search index..."):
                search_engine.build_index()
    
    # Main search interface
    col1, col2 = st.columns([4, 1])
    
    with col1:
        query = st.text_input(
            "Search for songs by title, artist, or lyrics:",
            placeholder="e.g., 'love songs', 'Beatles', 'happy melody'"
        )
    
    with col2:
        st.write("")  # Empty space to align button with input
        search_button = st.button("🔍 Search", type="primary")
    
    # Search suggestions
    if query and len(query) >= 2:
        suggestions = search_engine.get_search_suggestions(query)
        if suggestions:
            st.markdown("**Suggestions:** " + " • ".join([f"`{s}`" for s in suggestions]))
    
    # Perform search
    if query and (search_button or query):
        # Determine search fields
        search_fields = []
        if search_title:
            search_fields.append("title")
        if search_artist:
            search_fields.append("artist")
        if search_lyrics:
            search_fields.append("lyrics")
        
        if not search_fields:
            st.warning("Please select at least one search field.")
        else:
            with st.spinner("Searching..."):
                results = search_engine.search_songs(
                    query, 
                    search_fields=search_fields, 
                    top_n=max_results
                )
            
            # Display results
            if results:
                st.success(f"Found {len(results)} result(s) for '{query}':")
                
                # Results display
                for i, result in enumerate(results, 1):
                    with st.expander(f"{i}. **{result['title']}** by *{result['artist']}* (Score: {result['score']:.2f})"):
                        song_details = search_engine.get_song_details(result['song_id'])
                        if song_details:
                            col1, col2 = st.columns(2)
                            with col1:
                                st.write("**Title:**", song_details.get('title', 'N/A'))
                                st.write("**Artist:**", song_details.get('artist', 'N/A'))
                                st.write("**Song ID:**", song_details.get('song_id', 'N/A'))
                                
                                # Add music platform links
                                title = song_details.get('title', '')
                                artist = song_details.get('artist', '')
                                search_query = f"{title} {artist}".replace(" ", "+")
                                
                                st.markdown("**Listen on:**")
                                col_yt, col_spot = st.columns(2)
                                with col_yt:
                                    st.link_button("🎵 YouTube", f"https://www.youtube.com/results?search_query={search_query}")
                                with col_spot:
                                    st.link_button("🎧 Spotify", f"https://open.spotify.com/search/{search_query}")
                                    
                            with col2:
                                lyrics = song_details.get('lyrics', 'No lyrics available')
                                st.write("**Lyrics Preview:**")
                                # Show only first 100 characters to avoid copyright issues
                                preview = lyrics[:100] + "..." if len(lyrics) > 100 else lyrics
                                st.write(preview)
            else:
                st.warning("No results found. Try different keywords or check your search options.")
    
    # Footer
    st.markdown("---")
    st.markdown("*Built with Whoosh IR library and Streamlit*")

if __name__ == "__main__":
    main()