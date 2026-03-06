import pandas as pd
import numpy as np
import plotly.express as px
from dash import Dash, dcc, html

PATH = r'...'

COST_CATEGORIES = {
    'Zakupy spożywcze': ['DELIKATESY', 'ZABKA', 'BIEDRONKA', 'ALDI', 'Warzywny Ogrodek', 'LIDL', 'BLUMIS', 'KAUFLAND',
                         'OSIEDLOWY', 'MAJA', 'AUCHAN'],
    'Zakupy online': ['ALLEGRO', 'KAUFLAND.PL', 'EBAY', 'EMPIK', 'AMAZON', 'PAYU'],    
    'Ubezpieczenia': ['PZU'],
    'Gastronomia': ['SUSHI', 'PIZZA', 'BROWAR', 'RESTAURACJA', 'PIZZERIA', 'MCDONALDS', 'KFC', 'GYROS',
                    'COSTA COFFEE'], 
    'Paliwo': ['MOL', 'ORLEN', 'MOYA' , 'SHELL']   
}

def load_csv_dataset(path):

    # Data ingestion
    df = pd.read_csv(PATH, sep=',', quotechar='\"', encoding='Ansi')
    
    return df

def prepare_dataset(dataframe):

    # Assign dataframe
    df=dataframe

    # Set proper company name or transfer recipient basing on payment type
    conditions = [
        df["Typ transakcji"] == "Płatność kartą",
        df["Typ transakcji"] == "Płatność web - kod mobilny",
        df["Typ transakcji"] == "Przelew z rachunku",
        df["Typ transakcji"] == "Przelew na konto",
        df["Typ transakcji"] == "Zakup w terminalu - kod mobilny",
    ]
    choices = [df["Unnamed: 7"], df["Unnamed: 8"], df["Unnamed: 7"], df["Unnamed: 7"], df["Unnamed: 7"]]

    # Set clean company name or transfer recipient
    df["Nazwa"] = np.select(conditions, choices, default="unknown")
    df["Nazwa"] = df["Nazwa"].str.replace('Lokalizacja: Adres: ','')
    df["Nazwa"] = df["Nazwa"].str.replace('Nazwa odbiorcy: ','')

    # Assign payment to its category based on static (yet) dictionary
    conditions = [
        df["Nazwa"].str.contains('|'.join(keywords), case=False, na=False)
        for keywords in COST_CATEGORIES.values()
    ]

    choices = list(COST_CATEGORIES.keys())
    df["Kategoria"] = np.select(conditions, choices, default="Inne")

    # Select only relevant columns
    df = df[["Data operacji","Data waluty","Typ transakcji","Kwota","Waluta","Saldo po transakcji","Opis transakcji",
             "Nazwa", "Kategoria"]]

    # Remove deposits
    df = df[~df['Opis transakcji'].str.contains('OTWARCIE LOKATY')]

    # Only outcomes
    df = df[df['Kwota'] < 0]

    # Convert to postive amounts
    df["Kwota"] = pd.to_numeric(df["Kwota"]) * -1

    # Set month name
    df["Miesiac"] = pd.to_datetime(df["Data operacji"]).dt.strftime("%Y-%m")
    return df

def generate_charts(df):
    # Bar chart showing amounts per month groupped by category (stack)
    fig_stack = px.bar(df, x="Miesiac", y="Kwota", color="Kategoria", barmode="stack", title="Wydatki miesięczne",
                       category_orders={"Miesiac": sorted(df["Miesiac"].unique())}
                )
    # Bar chart showing amounts per month groupped by category
    fig_group = px.bar(df, x="Miesiac", y="Kwota", color="Kategoria", barmode="group",
                       title="Wydatki miesięczne według grup", category_orders={"month": sorted(df["Miesiac"].unique())})
    fig_names = px.bar(df, x="Miesiac", y="Kwota", color="Nazwa", barmode="group",
                       title="Wydatki miesięczne według nazw", category_orders={"month": sorted(df["Miesiac"].unique())})
    fig_pie = px.pie(df, names="Typ transakcji", values="Kwota")

    fig_stack.update_xaxes(type="category")
    fig_group.update_xaxes(type="category")

    # ── APP ──
    app = Dash(__name__)

    app.layout = html.Div([
        dcc.Graph(figure=fig_stack),
        dcc.Graph(figure=fig_names),
        dcc.Graph(figure=fig_group),
        dcc.Graph(figure=fig_pie),
    ], style={
            "display": "grid",
            "gridTemplateColumns": "1fr 1fr",   # 2 equal columns
            "gridTemplateRows": "1fr 1fr",      # 2 equal rows
            "gap": "10px",
            "height": "100vh",                  # full screen height
        })
    app.run(debug=True)

if __name__ == "__main__":
    df = load_csv_dataset(PATH)
    df_processed = prepare_dataset(df)
    generate_charts(df_processed)
