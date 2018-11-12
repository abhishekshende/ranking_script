
from flask import Flask
import ranking_script1

app = Flask(__name__)


@app.route('/')
def home():
    return 'Welcome to the ranking script.'

@app.route('/rank')
def ranking():
    ranking_script1.rankings()
    return 'ranked', 200


if __name__=='__main__':
    app.run(debug=True)