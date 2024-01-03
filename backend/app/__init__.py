from flask import Flask, jsonify
import pandas as pd 
import os

app = Flask(__name__)

@app.route('/')
def home():
    return "Welcome to Annies Data API :)"

@app.route('/data/<folder_name>/<file_name>', methods=['GET'])
def get_data(folder_name, file_name):
    try:
        file_path = os.path.join('data_processing/data_generation/prepared_data', folder_name, f'{file_name}.csv')
        if not os.path.exists(file_path):
            return jsonify({'error': 'File not found'}), 404
        data = pd.read_csv(file_path)
        return jsonify(data.to_dict()), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500
                        
if __name__ == '__main__':
    app.run(debug=True)