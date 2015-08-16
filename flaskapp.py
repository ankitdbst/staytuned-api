import os
from tvlistings import app
from datetime import datetime
from flask import Flask, request, flash, url_for, redirect, \
     render_template, abort, send_from_directory


@app.route("/test")
def test():
    return "<strong>It's Alive!</strong>"


if __name__ == '__main__':
    app.run()
