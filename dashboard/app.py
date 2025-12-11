from flask import Flask, render_template, jsonify, request
import pandas as pd
import json
from datetime import datetime
import os

app = Flask(__name__)

# Configuration
DATA_DIR = 'data/results'
REPORTS_DIR = 'data/results'

@app.route('/')
def index():
    """Render main dashboard"""
    return render_template('index.html')

@app.route('/api/flips')
def get_flips():
    """Get recent flips data"""
    try:
        # Find latest flips file
        flips_files = [f for f in os.listdir(DATA_DIR) if f.startswith('flips_detailed_')]
        if not flips_files:
            return jsonify([])
        
        latest_file = max(flips_files)
        df = pd.read_csv(os.path.join(DATA_DIR, latest_file))
        
        # Convert to dict for JSON
        flips_data = df.head(100).to_dict('records')  # Limit to 100 for demo
        
        return jsonify(flips_data)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/investors')
def get_investors():
    """Get investor data"""
    try:
        investor_files = [f for f in os.listdir(DATA_DIR) if f.startswith('investors_')]
        if not investor_files:
            return jsonify([])
        
        latest_file = max(investor_files)
        df = pd.read_csv(os.path.join(DATA_DIR, latest_file))
        
        # Add priority ranking
        df = df.sort_values('total_flips', ascending=False)
        investors_data = df.head(50).to_dict('records')
        
        return jsonify(investors_data)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/stats')
def get_stats():
    """Get dashboard statistics"""
    try:
        report_files = [f for f in os.listdir(REPORTS_DIR) if f.startswith('report_')]
        if not report_files:
            return jsonify({})
        
        latest_file = max(report_files)
        with open(os.path.join(REPORTS_DIR, latest_file), 'r') as f:
            stats = json.load(f)
        
        # Add additional stats
        stats['last_updated'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        return jsonify(stats)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/export', methods=['POST'])
def export_data():
    """Export data for selected investors"""
    try:
        data = request.json
        investor_ids = data.get('investor_ids', [])
        
        # Load investors
        investor_files = [f for f in os.listdir(DATA_DIR) if f.startswith('investors_')]
        if investor_files:
            latest_file = max(investor_files)
            df = pd.read_csv(os.path.join(DATA_DIR, latest_file))
            
            # Filter selected investors
            if investor_ids:
                df = df[df['investor_name'].isin(investor_ids)]
            
            # Save export
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            export_path = f'data/exports/selected_investors_{timestamp}.csv'
            os.makedirs('data/exports', exist_ok=True)
            df.to_csv(export_path, index=False)
            
            return jsonify({
                'success': True,
                'file': export_path,
                'count': len(df)
            })
        
        return jsonify({'error': 'No data found'}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/run-analysis', methods=['POST'])
def run_analysis():
    """Trigger new analysis run"""
    try:
        # This would trigger the analysis pipeline
        # For now, just return success
        return jsonify({
            'success': True,
            'message': 'Analysis started',
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    # Create necessary directories
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(REPORTS_DIR, exist_ok=True)
    
    # Create templates directory if it doesn't exist
    os.makedirs('dashboard/templates', exist_ok=True)
    
    app.run(debug=True, port=5000)
