import duckdb
import pandas as pd
import plotly.express as px

# load data from duckDB
con = duckdb.connect("wiki.db")
df = con.execute("SELECT * FROM wikipedia_events").fetch_df()
con.close()

# language plot
lang_counts = (
    df.groupby("wiki")
      .size()
      .reset_index(name="Number of Edits")
      .sort_values("Number of Edits", ascending=False)
      .head(30)
)

# making the languages more readable
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
    "eswiktionary": "Spanish Wiktionary",
    "metawiki": "Meta-Wiki",
    "mgwiktionary": "Malagasy Wiktionary",
    "ruwikinews": "Russian Wikinews"
}

lang_counts["Language"] = lang_counts["wiki"].map(wiki_mapping).fillna(lang_counts["wiki"])

fig_lang = px.bar(
    lang_counts,
    x="Language",
    y="Number of Edits",
    title="Edits by Language",
)

fig_lang.update_layout(xaxis_title="Language", yaxis_title="Number of Edits")

# Save to file
fig_lang.write_image("language_plot.png", scale=3)
print("Saved: language_plot.png")

fig_lang.show()

# type of edit plot
type_counts = (
    df.groupby("type")
      .size()
      .reset_index(name="Number of Edits")
      .sort_values("Number of Edits", ascending=False)
)

fig_type = px.bar(
    type_counts,
    x="type",
    y="Number of Edits",
    title="Edits by Change Type",
)

fig_type.update_layout(xaxis_title="Change Type", yaxis_title="Number of Edits")

# Save to file
fig_type.write_image("type_plot.png", scale=3)
print("Saved: type_plot.png")

fig_type.show()


