import plotly.graph_objects as go
import plotly.express as px
import streamlit as st

# Theme colors from .streamlit/config.toml
THEME_PRIMARY_COLOR = "#1E88E5"  # Blue
THEME_BACKGROUND_COLOR = "#F5F5F5"  # Light grey
THEME_SECONDARY_BACKGROUND_COLOR = "#FFFFFF"  # White
THEME_TEXT_COLOR = "#31333F"  # Dark grey

def create_themed_figure():
    """Create a Plotly figure with consistent theming based on Streamlit config"""
    fig = go.Figure()
    
    # Apply consistent theming
    fig.update_layout(
        font=dict(
            family="sans serif",  # Match Streamlit font
            color=THEME_TEXT_COLOR,
        ),
        paper_bgcolor='rgba(0,0,0,0)',  # Transparent background
        plot_bgcolor='rgba(0,0,0,0)',   # Transparent plot area
        margin=dict(l=40, r=40, t=50, b=40),
        colorway=[THEME_PRIMARY_COLOR, "#FF9800", "#4CAF50", "#F44336", "#9C27B0", "#00BCD4"],  # Primary color first, then complementary colors
    )
    
    # Update axes for consistent look
    fig.update_xaxes(
        gridcolor='rgba(0,0,0,0.1)',
        zerolinecolor='rgba(0,0,0,0.2)',
        title_font=dict(size=14),
    )
    
    fig.update_yaxes(
        gridcolor='rgba(0,0,0,0.1)',
        zerolinecolor='rgba(0,0,0,0.2)',
        title_font=dict(size=14),
    )
    
    return fig

def create_themed_histogram(df, x, nbins=30, title=None, xaxis_title=None, yaxis_title="Count"):
    """Create a themed histogram using Plotly Express"""
    fig = px.histogram(
        df,
        x=x,
        nbins=nbins,
        labels={x: xaxis_title if xaxis_title else x},
        title=title,
        color_discrete_sequence=[THEME_PRIMARY_COLOR],
    )
    
    # Apply theming
    fig.update_layout(
        font=dict(
            family="sans serif",
            color=THEME_TEXT_COLOR,
        ),
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        margin=dict(l=40, r=40, t=50, b=40),
        title_font=dict(size=16),
    )
    
    # Update axes
    fig.update_xaxes(
        title=xaxis_title,
        gridcolor='rgba(0,0,0,0.1)',
        title_font=dict(size=14),
    )
    
    fig.update_yaxes(
        title=yaxis_title,
        gridcolor='rgba(0,0,0,0.1)',
        title_font=dict(size=14),
    )
    
    return fig

def create_themed_bar(df, x, y, title=None, xaxis_title=None, yaxis_title=None, color=None, color_discrete_sequence=None):
    """Create a themed bar chart using Plotly Express"""
    if color_discrete_sequence is None:
        color_discrete_sequence = [THEME_PRIMARY_COLOR]
        
    fig = px.bar(
        df,
        x=x,
        y=y,
        title=title,
        color=color,
        color_discrete_sequence=color_discrete_sequence,
        labels={
            x: xaxis_title if xaxis_title else x,
            y: yaxis_title if yaxis_title else y,
        },
    )
    
    # Apply theming
    fig.update_layout(
        font=dict(
            family="sans serif",
            color=THEME_TEXT_COLOR,
        ),
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        margin=dict(l=40, r=40, t=50, b=40),
        title_font=dict(size=16),
    )
    
    # Update axes
    fig.update_xaxes(
        title=xaxis_title if xaxis_title else x,
        gridcolor='rgba(0,0,0,0.1)',
        title_font=dict(size=14),
    )
    
    fig.update_yaxes(
        title=yaxis_title if yaxis_title else y,
        gridcolor='rgba(0,0,0,0.1)',
        title_font=dict(size=14),
    )
    
    return fig

def create_themed_line(df, x, y, title=None, xaxis_title=None, yaxis_title=None, color=None, color_discrete_sequence=None):
    """Create a themed line chart using Plotly Express"""
    if color_discrete_sequence is None:
        color_discrete_sequence = [THEME_PRIMARY_COLOR]
        
    fig = px.line(
        df,
        x=x,
        y=y,
        title=title,
        color=color,
        color_discrete_sequence=color_discrete_sequence,
        labels={
            x: xaxis_title if xaxis_title else x,
            y: yaxis_title if yaxis_title else y,
        },
    )
    
    # Apply theming
    fig.update_layout(
        font=dict(
            family="sans serif",
            color=THEME_TEXT_COLOR,
        ),
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        margin=dict(l=40, r=40, t=50, b=40),
        title_font=dict(size=16),
    )
    
    # Update axes
    fig.update_xaxes(
        title=xaxis_title if xaxis_title else x,
        gridcolor='rgba(0,0,0,0.1)',
        title_font=dict(size=14),
    )
    
    fig.update_yaxes(
        title=yaxis_title if yaxis_title else y,
        gridcolor='rgba(0,0,0,0.1)',
        title_font=dict(size=14),
    )
    
    # Make lines a bit thicker for better visibility
    fig.update_traces(line=dict(width=2.5))
    
    return fig