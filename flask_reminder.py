from flask import Flask, render_template, request, redirect, url_for
from flask_sqlalchemy import SQLAlchemy
from pathlib import Path


app = Flask(__name__)
file_path = str(Path(__file__).parent.joinpath('database.db'))
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///'+file_path
db = SQLAlchemy(app)

class db_model(db.Model):
    id = db.Column(db.Integer, primary_key = True)
    text = db.Column(db.String(100))

    def __repr__(self):
        return self.text

with app.app_context():
    db.create_all()  # Creates the database tables

@app.route("/")
def index():
    available_items = db_model.query.all()
    return render_template('index.html', available_items=available_items)

@app.route('/add', methods=['POST'])
def add():
    model = db_model(text=request.form['itemadd'])
    db.session.add(model)
    db.session.commit()

    return redirect(url_for('index'))

@app.route('/drop_db_model', methods=['POST'])
def drop_db_model_table():
    db_model.__table__.drop(db.engine)
    with app.app_context():
        db.create_all()  # Creates the database tables

    return redirect(url_for('index'))

@app.route("/hello")
def hello():
    return "Hello World!"  +  '<a href="/"> go back to index </a>'