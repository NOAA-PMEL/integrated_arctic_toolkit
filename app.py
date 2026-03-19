from dash import Dash, html
from cache import cache
from layouts.main_layout2 import layout
import dash_design_kit as ddk
import os
# import callbacks.map_callbacks # importing is enough, will run the file which will register the callback

app = Dash(__name__)

# app.layout = layout
# This is the "Shell" that enables the editor
app.layout = layout

server = app.server # expose flask server for Gunicorn

# Right now caching is for SST data (it updates daily)
cache.init_app(app.server, config={
    "CACHE_TYPE": "RedisCache",
    "CACHE_REDIS_URL": os.environ.get("REDIS_URL", "redis://127.0.0.1:6379"),
    "CACHE_DEFAULT_TIMEOUT": 3600 # 1 hour - SST onlly updates daily anyway
})

if __name__ == "__main__":
    app.run(debug=True)