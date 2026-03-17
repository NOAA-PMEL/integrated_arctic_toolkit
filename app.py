from dash import Dash
from layouts.main_layout import layout
import callbacks.map_callbacks # importing is enough, will run the file which will register the callback

app = Dash(__name__)
app.layout = layout

server = app.server # expose flask server for Gunicorn

if __name__ == "__main__":
    app.run(debug=True)