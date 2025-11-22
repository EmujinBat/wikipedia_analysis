import duckdb
import pandas as pd
import plotly.express as px

# Connect to DuckDB and load data
con = duckdb.connect("wiki.db")
df = con.execute("SELECT * FROM wikipedia_events").fetchdf()

# Convert timestamp to datetime
df['datetime'] = pd.to_datetime(df['timestamp'], unit='s')
df['hour'] = df['datetime'].dt.hour

# Map wiki codes to languages
wiki_mapping = {
    "enwiki": "English",
    "frwiki": "French",
    "dewiki": "German",
    "eswiki": "Spanish",
    "itwiki": "Italian",
    "ruwiki": "Russian",
    "jawiki": "Japanese",
    "zhwiki": "Chinese",
    "plwiki": "Polish",
    "nlwiki": "Dutch",
    "ptwiki": "Portuguese",
    "arwiki": "Arabic",
    "svwiki": "Swedish",
    "ukwiki": "Ukrainian",
    "viwiki": "Vietnamese",
    "kowiki": "Korean",
    "trwiki": "Turkish",
    "cswiki": "Czech",
    "rowiki": "Romanian",
    "huwiki": "Hungarian",
    "fiwiki": "Finnish",
    "hewiki": "Hebrew",
    "idwiki": "Indonesian",
    "hrwiki": "Croatian",
    "srwiki": "Serbian",
    "dawiki": "Danish",
    "nowiki": "Norwegian",
    "thwiki": "Thai",
    "elwiki": "Greek",
    "commonswiki": "Wikimedia Commons",
    "simplewiki": "Simple English",
    "azwiki": "Azerbaijani",
    "etwiki": "Estonian",
    "wikidatawiki": "Wikidata",
    "enwiktionary": "English Wiktionary",
    "fawiki": "Persian",  
    "swwiki": "Swahili",
    "euwiki": "Basque",
    "cywiki": "Welsh",
    "iswiki": "Icelandic",
    "mkwiki": "Macedonian",
    "glwiki": "Galician",
    "bswiki": "Bosnian",
    "tewiki": "Telugu",
    "knwiki": "Kannada",
    "mlwiki": "Malayalam",
    "pawiki": "Punjabi",
    "guwiki": "Gujarati",
    "orwiki": "Odia",
    "bnwiki": "Bengali",
    "hiwiki": "Hindi",
    "mrwiki": "Marathi",
    "urwiki": "Urdu",
    "incubatorwiki": "Incubator",
    "eswiktionary": "Spanish Wiktionary"
}

df['language_name'] = df['wiki'].map(wiki_mapping).fillna(df['wiki'])

# Interactive Bar Chart: Edits by Language
lang_counts = df['language_name'].value_counts().reset_index()
lang_counts.columns = ['Language', 'Number of Edits']


fig_lang = px.bar(
    lang_counts,
    x='Language',
    y='Number of Edits',
    title='Edits by Language',
    hover_data={'Language': True, 'Number of Edits': True},
)
fig_lang.update_layout(xaxis_title='Language', yaxis_title='Number of Edits')
fig_lang.show()

# Interactive Bar Chart: Edits by Change Type
type_counts = df['type'].value_counts().reset_index()
type_counts.columns = ['Change Type', 'Number of Edits']

fig_type = px.bar(
    type_counts,
    x='Change Type',
    y='Number of Edits',
    title='Edits by Change Type',
    hover_data={'Change Type': True, 'Number of Edits': True},
)
fig_type.update_layout(xaxis_title='Change Type', yaxis_title='Number of Edits')
fig_type.show()

# Interactive Line Chart: Edits by Hour of Day

# Convert timestamp column to datetime in UTC
df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s', utc=True)
df = df.dropna(subset=['timestamp'])

# Create proper hour bins
df['hour'] = df['timestamp'].dt.hour

hour_counts = df.groupby('hour').size().reset_index(name='Number of Edits')
# group by actual timeline hours


fig_hour = px.line(
    hour_counts,
    x='hour',
    y='Number of Edits',
    title='Edits by Hour (UTC)',
    markers=True,
    hover_data={'hour': True, 'Number of Edits': True}
)

fig_hour.update_layout(xaxis_title='Hour (UTC)', yaxis_title='Number of Edits')
fig_hour.show()

