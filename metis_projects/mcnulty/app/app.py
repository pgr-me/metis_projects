import flask
from sklearn.linear_model import LogisticRegression
import numpy as np



# Train a model at the beginning; this guy will stay in memory the whole
# time our server is up!

# The example training data has a single feature and for all training X
# values from 1 to 500 the true response is 0; for all X values from 500
# to 1000 the true response is 1. Pretty simple decision boundary here,
# the model is if you encounter x < 500, classify as 0, if 500 < x,
# classify as 1.

# You could also load a pickled model rather than training on server
# launch, which would be more typical.

X = np.linspace(1, 1000, 50).reshape(-1,1)
Y = np.zeros(50,)
Y[25:] = np.ones(25,)
PREDICTOR = LogisticRegression().fit(X,Y)


# Initialize the app

app = flask.Flask(__name__)


# An example of routing:
# If they go to the page "/" (this means a GET request
# to the page http://127.0.0.1:5000/), return a simple
# page that says the site is up!

@app.route("/")
def hello():
    return "It's alive!!!"


# Let's turn this into an API where you can post input data and get
# back output data after some calculations.

# If a user makes a POST request to http://127.0.0.1:5000/predict, and
# sends an X vector (to predict a class y_pred) with it as its data,
# we will use our trained LogisticRegression model to make a
# prediction and send back another JSON with the answer. You can use
# this to make interactive visualizations.

@app.route("/predict", methods=["POST"])
def predict():

    # read the data that came with the POST request as a dict
    data = flask.request.json

    # let's convert this into a numpy array so that we can
    # stick it into our model
    x = np.array(data["example"]).reshape(-1,1)

    # Classify!
    y_pred = PREDICTOR.predict(x)

    # Turn the result into a simple list so we can put it in
    # a json (json won't understand numpy arrays)
    y_pred = list(y_pred)

    # Put the result in a nice dict so we can send it as json
    results = {"predicted": y_pred}

    # Return a response with a json in it
    # flask has a quick function for that that takes a dict
    return flask.jsonify(results)


# Start the server, continuously listen to requests.
# We'll have a running web app!

# For local development:
app.run(debug=True)

# For public web serving:
# app.run(host='0.0.0.0')