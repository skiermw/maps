from flask import Flask, request, session, g, redirect, url_for, \
     abort, render_template, flash

# create our little application :)
app = Flask(__name__)
app.config.from_object(__name__)


@app.route('/')
def show_map():
    return render_template('show_thornbrook.html')


if __name__ == '__main__':
    app.run( host='0.0.0.0',port=5555)
	#app.run()