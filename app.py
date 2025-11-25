# Hide console window for executable
import ctypes
import os
import sys
import urllib.parse
from pymongo import MongoClient, ASCENDING, DESCENDING
from bson import ObjectId
import urllib.parse
import os
import threading
import time
import webbrowser
from flask import Flask, render_template, request, jsonify, redirect, url_for, session
from pymongo import MongoClient, ASCENDING, DESCENDING
from datetime import datetime, timedelta
from bson import ObjectId
from collections import defaultdict
from flask_cors import CORS
import statistics

def hide_console():
    """Hide console window in executable"""
    if os.name == 'nt':  # Windows
        ctypes.windll.user32.ShowWindow(ctypes.windll.kernel32.GetConsoleWindow(), 0)
    elif os.name == 'posix':  # Linux/Mac
        pass  # No need to hide on Linux/Mac


# Hide console immediately
hide_console()



# Fix for executable - get the correct path to resources
def resource_path(relative_path):
    """Get absolute path to resource, works for dev and for PyInstaller"""
    try:
        # PyInstaller creates a temp folder and stores path in _MEIPASS
        base_path = sys._MEIPASS
    except Exception:
        base_path = os.path.abspath(".")

    return os.path.join(base_path, relative_path)


# Create Flask app with proper template path
template_dir = resource_path('templates') if hasattr(sys, '_MEIPASS') else 'templates'
app = Flask(__name__, template_folder=template_dir)
CORS(app)
app.secret_key = 'your-secret-key-here'  # Required for session management


# Add cache control headers to disable caching
@app.after_request
def add_header(response):
    """
    Add headers to prevent caching - this will force browser to always load fresh content
    """
    response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate, max-age=0'
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '0'
    return response


# MongoDB connection with connection pooling and timeout settings


try:
    # -------------------------
    # Get username & password
    # -------------------------
    username = os.environ.get('MONGODB_USERNAME', 'adityabhoir983_db_user')
    password = os.environ.get('MONGODB_PASSWORD', 'HiV2rwczhpH0Cpjq')
    encoded_password = urllib.parse.quote_plus(password)

    # -------------------------
    # Correct Atlas connection string
    # -------------------------
    connection_string = (
        f"mongodb+srv://{username}:{encoded_password}"
        "@cluster0.aavnxbi.mongodb.net/pharmacy_db"
        "?retryWrites=true&w=majority&appName=Cluster0"
    )

    # -------------------------
    # Connect to Atlas
    # -------------------------
    client = MongoClient(
        connection_string,
        maxPoolSize=50,
        connectTimeoutMS=30000,
        socketTimeoutMS=30000,
        serverSelectionTimeoutMS=30000
    )

    # Test connection
    client.admin.command("ping")

    # Select DB & collections
    db = client["pharmacy_db"]
    transactions_collection = db["transactions"]
    user_collection = db["user"]

    print("✓ MongoDB connected successfully")

except Exception as e:
    print(f"✗ MongoDB connection failed: {e}")

    # -----------------------------------
    # Fallback dummy collections
    # -----------------------------------
    class DummyCollection:
        def find(self, *args, **kwargs): return []
        def find_one(self, *args, **kwargs): return None

        def insert_one(self, *args, **kwargs):
            class Result:
                inserted_id = ObjectId()
            return Result()

        def update_one(self, *args, **kwargs):
            class Result:
                modified_count = 1
            return Result()

        def delete_one(self, *args, **kwargs):
            class Result:
                deleted_count = 1
            return Result()

        def distinct(self, *args, **kwargs): return []
        def create_index(self, *args, **kwargs): pass
        def aggregate(self, *args, **kwargs): return []

    transactions_collection = DummyCollection()
    user_collection = DummyCollection()

# ---------------------------------------
# Create Indexes (safe even if fallback)
# ---------------------------------------
try:
    transactions_collection.create_index([("date", ASCENDING), ("type", ASCENDING)])
    transactions_collection.create_index([("date", ASCENDING)])
    transactions_collection.create_index([("type", ASCENDING)])
    transactions_collection.create_index([("created_at", DESCENDING)])
    print("✓ Database indexes created for optimal performance")
except Exception as e:
    print(f"Note: Index creation failed (may already exist): {e}")


# Cache for frequently accessed data
category_cache = {}
summary_cache = {}
CACHE_TIMEOUT = 300  # 5 minutes

# Income categories
INCOME_CATEGORIES = [
    'Ulhasnagar',
    'Kalyan',
    'Varap',
    'Varap (kulfee)'
]

# Expense categories
EXPENSE_CATEGORIES = [
    'Amul',
    'Hamour Ice Cream',
    'Bharkadevi Ice Cream',
    'Quality Walls',
    'Patel Kulfi',
    'Cold Drinks',
    'Bisleri',
    'Milk',
    'Dry Fruit',
    'Shop Rent',
    'Light Bill',
    'Employee Salary',
    'Cup',
    'Glass',
    'Spoon',
    'Straw',
    'Container',
    'Lid',
    'Tissue Paper',
    'Carry Bag',
    'Maintenance',
    'Nasta',
    'Flavours',
    'Utensils (Glass)',
    'Dahi',
    'Custard Powder',
    'Shev',
    'Gas',
    'Sugar',
    'Sticks',
    'Other'
]

# Shop names for expenses
SHOP_NAMES = [
    'Ulhasnagar',
    'Kalyan',
    'Varap',
    'Varap (kulfee)'
]


def clear_cache_periodically():
    """Clear cache periodically to free memory"""
    while True:
        time.sleep(CACHE_TIMEOUT)
        category_cache.clear()
        summary_cache.clear()


# Start cache clearing thread
cache_thread = threading.Thread(target=clear_cache_periodically, daemon=True)
cache_thread.start()


def get_cached_categories(category_type):
    """Get categories with caching for better performance"""
    cache_key = f"categories_{category_type}"
    if cache_key in category_cache:
        return category_cache[cache_key]

    if category_type == 'income':
        base_categories = INCOME_CATEGORIES
        try:
            db_categories = transactions_collection.distinct('category', {'type': 'income'})
        except:
            db_categories = []
    else:
        base_categories = EXPENSE_CATEGORIES
        try:
            db_categories = transactions_collection.distinct('category', {'type': 'expense'})
        except:
            db_categories = []

    all_categories = sorted(list(set(base_categories + db_categories)))
    category_cache[cache_key] = all_categories
    return all_categories


def login_required(f):
    """Decorator to check if user is logged in"""
    def decorated_function(*args, **kwargs):
        if not session.get('logged_in'):
            return redirect(url_for('show_login'))
        return f(*args, **kwargs)
    decorated_function.__name__ = f.__name__
    return decorated_function


@app.route('/')
def show_login():
    # Clear any existing session
    session.clear()
    try:
        return render_template('login.html')
    except Exception as e:
        return f"Error loading login page: {e}", 500


@app.route('/login', methods=['POST'])
def login():
    data = request.json
    username_or_email = data.get('username_or_email')
    password = data.get('password')

    # Simplified login: any non-empty input redirects to dashboard (no credential check)
    if username_or_email and password:
        # Set session variable
        session['logged_in'] = True
        session['username'] = username_or_email
        return jsonify({'redirect': '/dashboard'})
    else:
        return jsonify({'error': 'Username and password are required'}), 400


@app.route('/logout')
def logout():
    """Logout user and clear session"""
    session.clear()
    return redirect(url_for('show_login'))


@app.route('/dashboard')
@login_required
def dashboard():
    try:
        return render_template('dashboard.html')
    except Exception as e:
        return f"Error loading dashboard: {e}", 500


@app.route('/income')
@login_required
def income():
    try:
        return render_template('income.html')
    except Exception as e:
        return f"Error loading income page: {e}", 500


@app.route('/expense')
@login_required
def expense():
    try:
        return render_template('expense.html')
    except Exception as e:
        return f"Error loading expense page: {e}", 500


@app.route('/reports')
@login_required
def reports():
    try:
        return render_template('reports.html')
    except Exception as e:
        return f"Error loading reports page: {e}", 500


@app.route('/profit')
@login_required
def profit():
    try:
        return render_template('profit.html')
    except Exception as e:
        return f"Error loading profit page: {e}", 500


@app.route('/shop')
@login_required
def shop():
    try:
        return render_template('shop.html')
    except Exception as e:
        return f"Error loading shop page: {e}", 500


@app.route('/history')
@login_required
def history():
    try:
        return render_template('history.html')
    except Exception as e:
        return f"Error loading history page: {e}", 500


@app.route('/api/check-auth')
def check_auth():
    """Check if user is authenticated"""
    if session.get('logged_in'):
        return jsonify({'authenticated': True})
    else:
        return jsonify({'authenticated': False}), 401


@app.route('/api/transactions', methods=['GET'])
@login_required
def get_transactions():
    date = request.args.get('date')

    try:
        # Use projection to fetch only required fields
        projection = {'_id': 1, 'type': 1, 'category': 1, 'amount': 1, 'date': 1, 'description': 1, 'created_at': 1,
                      'shop_name': 1, 'unit': 1, 'qty': 1}

        if date:
            transactions = list(transactions_collection.find(
                {'date': date},
                projection
            ).sort('created_at', DESCENDING))
        else:
            transactions = list(transactions_collection.find(
                {},
                projection
            ).sort('created_at', DESCENDING).limit(1000))  # Limit for performance

        # Convert ObjectId to string
        for transaction in transactions:
            transaction['id'] = str(transaction['_id'])
            del transaction['_id']

        return jsonify(transactions)
    except Exception as e:
        return jsonify({'error': f'Database error: {str(e)}'}), 500


@app.route('/api/transactions', methods=['POST'])
@login_required
def add_transaction():
    data = request.json
    data['created_at'] = datetime.now()

    try:
        result = transactions_collection.insert_one(data)
        # Clear relevant caches
        if 'date' in data:
            summary_cache.clear()
        return jsonify({
            'message': 'Transaction added successfully',
            'id': str(result.inserted_id)
        }), 201
    except Exception as e:
        return jsonify({'error': f'Database error: {str(e)}'}), 500


@app.route('/api/transactions/<transaction_id>', methods=['PUT'])
@login_required
def update_transaction(transaction_id):
    data = request.json

    try:
        result = transactions_collection.update_one(
            {'_id': ObjectId(transaction_id)},
            {'$set': data}
        )

        if result.modified_count > 0:
            summary_cache.clear()  # Clear cache on update
            return jsonify({'message': 'Transaction updated successfully'})
        else:
            return jsonify({'error': 'Transaction not found or no changes'}), 404
    except Exception as e:
        return jsonify({'error': f'Database error: {str(e)}'}), 500


@app.route('/api/transactions/<transaction_id>', methods=['DELETE'])
@login_required
def delete_transaction(transaction_id):
    try:
        result = transactions_collection.delete_one({'_id': ObjectId(transaction_id)})

        if result.deleted_count > 0:
            summary_cache.clear()  # Clear cache on delete
            return jsonify({'message': 'Transaction deleted successfully'})
        else:
            return jsonify({'error': 'Transaction not found'}), 404
    except Exception as e:
        return jsonify({'error': f'Database error: {str(e)}'}), 500


@app.route('/api/transactions/date-range', methods=['GET'])
@login_required
def get_transactions_by_date_range():
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    transaction_type = request.args.get('type', 'all')

    query = {'date': {'$gte': start_date, '$lte': end_date}}
    if transaction_type != 'all':
        query['type'] = transaction_type

    try:
        # Use projection for better performance
        projection = {'_id': 1, 'type': 1, 'category': 1, 'amount': 1, 'date': 1, 'description': 1, 'shop_name': 1,
                      'unit': 1, 'qty': 1}

        transactions = list(transactions_collection.find(
            query,
            projection
        ).sort('date', DESCENDING))

        for transaction in transactions:
            transaction['id'] = str(transaction['_id'])
            del transaction['_id']

        return jsonify(transactions)
    except Exception as e:
        return jsonify({'error': f'Database error: {str(e)}'}), 500


@app.route('/api/dashboard/summary', methods=['GET'])
@login_required
def get_dashboard_summary():
    date = request.args.get('date')

    # Use cache for frequently accessed data
    cache_key = f"dashboard_summary_{date}"
    if cache_key in summary_cache:
        return jsonify(summary_cache[cache_key])

    try:
        # Use aggregation pipeline for faster calculations
        pipeline = [
            {'$match': {'date': date}},
            {'$group': {
                '_id': '$type',
                'total': {'$sum': '$amount'}
            }}
        ]

        result = list(transactions_collection.aggregate(pipeline))

        total_income = 0
        total_expense = 0

        for item in result:
            if item['_id'] == 'income':
                total_income = item['total']
            elif item['_id'] == 'expense':
                total_expense = item['total']

        net_profit = total_income - total_expense

        response_data = {
            'total_income': total_income,
            'total_expense': total_expense,
            'net_profit': net_profit
        }

        # Cache the result
        summary_cache[cache_key] = response_data
        return jsonify(response_data)
    except Exception as e:
        return jsonify({'error': f'Database error: {str(e)}'}), 500


@app.route('/api/reminders/missing-income', methods=['GET'])
@login_required
def get_missing_income_dates():
    month = request.args.get('month')
    if not month:
        return jsonify({'error': 'Month parameter is required'}), 400

    year, month_num = map(int, month.split('-'))
    start_date = f"{year}-{month_num:02d}-01"
    end_date = f"{year}-{month_num:02d}-{get_days_in_month(year, month_num)}"

    # Get all dates in the month
    all_dates = get_all_dates_in_month(year, month_num)

    try:
        # Use distinct for faster date retrieval
        income_dates = set(transactions_collection.distinct(
            'date',
            {
                'date': {'$gte': start_date, '$lte': end_date},
                'type': 'income'
            }
        ))
    except:
        income_dates = set()

    # Find missing dates
    missing_dates = [date for date in all_dates if date not in income_dates]

    return jsonify({
        'month': month,
        'missing_dates': missing_dates,
        'total_days': len(all_dates),
        'days_with_income': len(income_dates),
        'days_without_income': len(missing_dates)
    })


@app.route('/api/reports/monthly-summary', methods=['GET'])
@login_required
def get_monthly_summary():
    month = request.args.get('month')
    if not month:
        return jsonify({'error': 'Month parameter is required'}), 400

    year, month_num = map(int, month.split('-'))
    start_date = f"{year}-{month_num:02d}-01"
    end_date = f"{year}-{month_num:02d}-{get_days_in_month(year, month_num)}"

    try:
        # Use aggregation for faster calculations
        pipeline = [
            {'$match': {'date': {'$gte': start_date, '$lte': end_date}}},
            {'$group': {
                '_id': {'type': '$type', 'category': '$category', 'date': '$date'},
                'amount': {'$sum': '$amount'}
            }}
        ]

        aggregated_data = list(transactions_collection.aggregate(pipeline))
    except:
        aggregated_data = []

    # Process aggregated data
    total_income = 0
    total_expense = 0
    income_by_category = defaultdict(float)
    expense_by_category = defaultdict(float)
    daily_breakdown = defaultdict(lambda: {'income': 0, 'expense': 0, 'profit': 0})

    for item in aggregated_data:
        amount = item['amount']
        trans_type = item['_id']['type']
        category = item['_id']['category']
        date = item['_id']['date']

        if trans_type == 'income':
            total_income += amount
            income_by_category[category] += amount
            daily_breakdown[date]['income'] += amount
        else:
            total_expense += amount
            expense_by_category[category] += amount
            daily_breakdown[date]['expense'] += amount

        daily_breakdown[date]['profit'] = daily_breakdown[date]['income'] - daily_breakdown[date]['expense']

    net_profit = total_income - total_expense
    days_in_month = get_days_in_month(year, month_num)

    return jsonify({
        'month': month,
        'total_income': total_income,
        'total_expense': total_expense,
        'net_profit': net_profit,
        'avg_daily_income': total_income / days_in_month if days_in_month > 0 else 0,
        'avg_daily_expense': total_expense / days_in_month if days_in_month > 0 else 0,
        'avg_daily_profit': net_profit / days_in_month if days_in_month > 0 else 0,
        'income_by_category': dict(income_by_category),
        'expense_by_category': dict(expense_by_category),
        'daily_breakdown': dict(daily_breakdown),
        'transaction_count': len(aggregated_data)
    })


@app.route('/api/profit-analysis', methods=['GET'])
@login_required
def get_profit_analysis():
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    analysis_type = request.args.get('type', 'daily')

    if not start_date or not end_date:
        return jsonify({'error': 'Start date and end date are required'}), 400

    try:
        # Use aggregation for better performance
        pipeline = [
            {'$match': {'date': {'$gte': start_date, '$lte': end_date}}},
            {'$group': {
                '_id': {'type': '$type', 'category': '$category', 'date': '$date'},
                'amount': {'$sum': '$amount'}
            }}
        ]

        aggregated_data = list(transactions_collection.aggregate(pipeline))
    except:
        aggregated_data = []

    # Process data
    total_income = 0
    total_expense = 0
    income_by_category = defaultdict(float)
    expense_by_category = defaultdict(float)
    daily_breakdown = defaultdict(lambda: {'income': 0, 'expense': 0, 'profit': 0})

    for item in aggregated_data:
        amount = item['amount']
        trans_type = item['_id']['type']
        category = item['_id']['category']
        date = item['_id']['date']

        if trans_type == 'income':
            total_income += amount
            income_by_category[category] += amount
            daily_breakdown[date]['income'] += amount
        else:
            total_expense += amount
            expense_by_category[category] += amount
            daily_breakdown[date]['expense'] += amount

        daily_breakdown[date]['profit'] = daily_breakdown[date]['income'] - daily_breakdown[date]['expense']

    net_profit = total_income - total_expense

    # Sort daily breakdown by date
    daily_breakdown = dict(sorted(daily_breakdown.items()))

    # Calculate advanced analytics
    analytics = calculate_advanced_analytics(daily_breakdown)

    # Generate breakdown based on analysis type
    breakdown = get_breakdown_by_type(aggregated_data, analysis_type)

    # Calculate additional metrics
    profit_margin = (net_profit / total_income * 100) if total_income > 0 else 0
    expense_ratio = (total_expense / total_income * 100) if total_income > 0 else 0

    return jsonify({
        'total_income': total_income,
        'total_expense': total_expense,
        'net_profit': net_profit,
        'profit_margin': profit_margin,
        'expense_ratio': expense_ratio,
        'income_by_category': dict(income_by_category),
        'expense_by_category': dict(expense_by_category),
        'daily_breakdown': daily_breakdown,
        'breakdown': breakdown,
        'analytics': analytics,
        'analysis_type': analysis_type,
        'date_range': {
            'start': start_date,
            'end': end_date
        },
        'transaction_count': len(aggregated_data)
    })


@app.route('/api/reports/shop-wise', methods=['GET'])
@login_required
def get_shop_wise_report():
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    month = request.args.get('month')
    shop_name = request.args.get('shop_name')

    # Support both month-based and date range-based queries
    if month:
        year, month_num = map(int, month.split('-'))
        start_date = f"{year}-{month_num:02d}-01"
        end_date = f"{year}-{month_num:02d}-{get_days_in_month(year, month_num)}"
    elif not start_date or not end_date:
        return jsonify({'error': 'Either month or start_date and end_date are required'}), 400

    try:
        # Base query for date range
        date_query = {'date': {'$gte': start_date, '$lte': end_date}}

        # Get income for the shop (income categories are shop names)
        income_query = {**date_query, 'type': 'income'}
        if shop_name:
            income_query['category'] = shop_name

        # Get expenses for the shop
        expense_query = {**date_query, 'type': 'expense'}
        if shop_name:
            expense_query['shop_name'] = shop_name

        # Use aggregation for better performance
        income_pipeline = [
            {'$match': income_query},
            {'$group': {
                '_id': {'category': '$category', 'date': '$date'},
                'amount': {'$sum': '$amount'}
            }}
        ]

        expense_pipeline = [
            {'$match': expense_query},
            {'$group': {
                '_id': {'category': '$category', 'shop_name': '$shop_name', 'date': '$date'},
                'amount': {'$sum': '$amount'}
            }}
        ]

        income_data = list(transactions_collection.aggregate(income_pipeline))
        expense_data = list(transactions_collection.aggregate(expense_pipeline))

    except Exception as e:
        return jsonify({'error': f'Database error: {str(e)}'}), 500

    # Process data
    total_income = 0
    total_expense = 0
    income_by_shop = defaultdict(float)
    expense_by_category = defaultdict(float)
    daily_breakdown = defaultdict(lambda: {'income': 0, 'expense': 0, 'profit': 0})

    # Process income data
    for item in income_data:
        amount = item['amount']
        category = item['_id']['category']
        date = item['_id']['date']

        total_income += amount
        income_by_shop[category] += amount
        daily_breakdown[date]['income'] += amount
        daily_breakdown[date]['profit'] += amount

    # Process expense data
    for item in expense_data:
        amount = item['amount']
        category = item['_id']['category']
        shop = item['_id'].get('shop_name', 'Unknown')
        date = item['_id']['date']

        total_expense += amount
        expense_by_category[category] += amount
        daily_breakdown[date]['expense'] += amount
        daily_breakdown[date]['profit'] -= amount

    net_profit = total_income - total_expense
    days_in_period = (datetime.strptime(end_date, '%Y-%m-%d') - datetime.strptime(start_date, '%Y-%m-%d')).days + 1

    return jsonify({
        'start_date': start_date,
        'end_date': end_date,
        'shop_name': shop_name,
        'total_income': total_income,
        'total_expense': total_expense,
        'net_profit': net_profit,
        'avg_daily_income': total_income / days_in_period if days_in_period > 0 else 0,
        'avg_daily_expense': total_expense / days_in_period if days_in_period > 0 else 0,
        'avg_daily_profit': net_profit / days_in_period if days_in_period > 0 else 0,
        'income_by_shop': dict(income_by_shop),
        'expense_by_category': dict(expense_by_category),
        'daily_breakdown': dict(daily_breakdown),
        'transaction_count': len(income_data) + len(expense_data)
    })


@app.route('/api/shops/with-income', methods=['GET'])
@login_required
def get_shops_with_income():
    """Get all shops that have income entries (income categories)"""
    try:
        shops = transactions_collection.distinct('category', {'type': 'income'})
        # Also include predefined income categories
        all_shops = sorted(list(set(INCOME_CATEGORIES + shops)))
        return jsonify(all_shops)
    except Exception as e:
        return jsonify(INCOME_CATEGORIES)  # Fallback to predefined


@app.route('/api/shop/analysis', methods=['GET'])
@login_required
def get_shop_analysis():
    """Get comprehensive shop analysis for the shop page"""
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    shop_name = request.args.get('shop_name')

    if not start_date or not end_date:
        return jsonify({'error': 'Start date and end date are required'}), 400

    try:
        # Get data for all shops if no specific shop is selected
        if not shop_name:
            # Aggregate data for all shops
            shops_data = {}
            total_income = 0
            total_expense = 0

            for shop in INCOME_CATEGORIES:
                shop_response = get_shop_wise_report()
                shop_data = shop_response.get_json()
                shops_data[shop] = shop_data
                total_income += shop_data.get('total_income', 0)
                total_expense += shop_data.get('total_expense', 0)

            net_profit = total_income - total_expense

            return jsonify({
                'shops': shops_data,
                'total_income': total_income,
                'total_expense': total_expense,
                'net_profit': net_profit,
                'profit_margin': (net_profit / total_income * 100) if total_income > 0 else 0
            })
        else:
            # Get data for specific shop
            shop_data_response = get_shop_wise_report()
            shop_data = shop_data_response.get_json()

            # Calculate additional metrics for shop analysis
            profit_margin = (shop_data['net_profit'] / shop_data['total_income'] * 100) if shop_data[
                                                                                               'total_income'] > 0 else 0

            # Calculate performance metrics
            daily_profits = [day['profit'] for day in shop_data['daily_breakdown'].values()]
            avg_daily_profit = statistics.mean(daily_profits) if daily_profits else 0
            best_day = max(daily_profits) if daily_profits else 0
            worst_day = min(daily_profits) if daily_profits else 0

            return jsonify({
                'shop_data': shop_data,
                'profit_margin': profit_margin,
                'performance_metrics': {
                    'avg_daily_profit': avg_daily_profit,
                    'best_day_profit': best_day,
                    'worst_day_profit': worst_day,
                    'profitable_days': sum(1 for profit in daily_profits if profit > 0),
                    'total_days': len(daily_profits)
                }
            })

    except Exception as e:
        return jsonify({'error': f'Error generating shop analysis: {str(e)}'}), 500


def calculate_advanced_analytics(daily_breakdown):
    """Calculate advanced analytics for profit analysis"""
    if not daily_breakdown:
        return {
            'avg_daily_profit': 0,
            'best_day_profit': 0,
            'worst_day_profit': 0,
            'profit_consistency': 0,
            'profit_volatility': 0,
            'profit_trend': 0,
            'profitable_days': 0,
            'loss_days': 0,
            'profit_distribution': {
                'high_profit': 0,
                'good_profit': 0,
                'medium_profit': 0,
                'low_profit': 0,
                'loss': 0
            }
        }

    profit_values = [day['profit'] for day in daily_breakdown.values()]

    # Basic metrics
    try:
        avg_daily_profit = statistics.mean(profit_values) if profit_values else 0
        best_day_profit = max(profit_values) if profit_values else 0
        worst_day_profit = min(profit_values) if profit_values else 0
    except:
        avg_daily_profit = 0
        best_day_profit = 0
        worst_day_profit = 0

    # Profit consistency (percentage of profitable days)
    profitable_days = sum(1 for profit in profit_values if profit > 0)
    profit_consistency = (profitable_days / len(profit_values)) * 100 if profit_values else 0

    # Profit volatility (standard deviation)
    try:
        profit_volatility = statistics.stdev(profit_values) if len(profit_values) > 1 else 0
    except:
        profit_volatility = 0

    # Profit trend (linear regression slope)
    profit_trend = calculate_profit_trend(list(daily_breakdown.values()))

    # Loss days
    loss_days = sum(1 for profit in profit_values if profit < 0)

    # Profit distribution by ranges
    profit_distribution = {
        'high_profit': sum(1 for profit in profit_values if profit > 50000),
        'good_profit': sum(1 for profit in profit_values if 25000 <= profit <= 50000),
        'medium_profit': sum(1 for profit in profit_values if 10000 <= profit < 25000),
        'low_profit': sum(1 for profit in profit_values if 0 <= profit < 10000),
        'loss': loss_days
    }

    return {
        'avg_daily_profit': avg_daily_profit,
        'best_day_profit': best_day_profit,
        'worst_day_profit': worst_day_profit,
        'profit_consistency': profit_consistency,
        'profit_volatility': profit_volatility,
        'profit_trend': profit_trend,
        'profitable_days': profitable_days,
        'loss_days': loss_days,
        'total_days': len(profit_values),
        'profit_distribution': profit_distribution
    }


def calculate_profit_trend(daily_data):
    """Calculate profit trend using linear regression"""
    if len(daily_data) < 2:
        return 0

    profits = [day['profit'] for day in daily_data]
    n = len(profits)

    # Simple linear regression
    x = list(range(n))
    y = profits

    sum_x = sum(x)
    sum_y = sum(y)
    sum_xy = sum(x[i] * y[i] for i in range(n))
    sum_x2 = sum(xi * xi for xi in x)

    try:
        slope = (n * sum_xy - sum_x * sum_y) / (n * sum_x2 - sum_x * sum_x)
        return slope
    except ZeroDivisionError:
        return 0


def get_breakdown_by_type(aggregated_data, analysis_type):
    """Get breakdown by week, month, or year"""
    if analysis_type == 'weekly':
        return get_weekly_breakdown(aggregated_data)
    elif analysis_type == 'monthly':
        return get_monthly_breakdown(aggregated_data)
    elif analysis_type == 'yearly':
        return get_yearly_breakdown(aggregated_data)
    else:
        return {}


def get_weekly_breakdown(aggregated_data):
    weekly_data = defaultdict(lambda: {'income': 0, 'expense': 0, 'profit': 0})

    for item in aggregated_data:
        date_str = item['_id']['date']
        try:
            date = datetime.strptime(date_str, '%Y-%m-%d')
            year, week, _ = date.isocalendar()
            week_key = f"{year}-W{week:02d}"

            amount = item['amount']
            if item['_id']['type'] == 'income':
                weekly_data[week_key]['income'] += amount
            else:
                weekly_data[week_key]['expense'] += amount

            weekly_data[week_key]['profit'] = weekly_data[week_key]['income'] - weekly_data[week_key]['expense']
        except:
            continue

    return dict(sorted(weekly_data.items()))


def get_monthly_breakdown(aggregated_data):
    monthly_data = defaultdict(lambda: {'income': 0, 'expense': 0, 'profit': 0})

    for item in aggregated_data:
        date_str = item['_id']['date']
        try:
            date = datetime.strptime(date_str, '%Y-%m-%d')
            month_key = date.strftime('%Y-%m')

            amount = item['amount']
            if item['_id']['type'] == 'income':
                monthly_data[month_key]['income'] += amount
            else:
                monthly_data[month_key]['expense'] += amount

            monthly_data[month_key]['profit'] = monthly_data[month_key]['income'] - monthly_data[month_key]['expense']
        except:
            continue

    return dict(sorted(monthly_data.items()))


def get_yearly_breakdown(aggregated_data):
    yearly_data = defaultdict(lambda: {'income': 0, 'expense': 0, 'profit': 0})

    for item in aggregated_data:
        date_str = item['_id']['date']
        try:
            date = datetime.strptime(date_str, '%Y-%m-%d')
            year_key = date.strftime('%Y')

            amount = item['amount']
            if item['_id']['type'] == 'income':
                yearly_data[year_key]['income'] += amount
            else:
                yearly_data[year_key]['expense'] += amount

            yearly_data[year_key]['profit'] = yearly_data[year_key]['income'] - yearly_data[year_key]['expense']
        except:
            continue

    return dict(sorted(yearly_data.items()))


@app.route('/api/categories/income', methods=['GET'])
@login_required
def get_income_categories():
    return jsonify(get_cached_categories('income'))


@app.route('/api/categories/expense', methods=['GET'])
@login_required
def get_expense_categories():
    return jsonify(get_cached_categories('expense'))


@app.route('/api/shops', methods=['GET'])
@login_required
def get_shop_names():
    """Get available shop names from predefined list + database"""
    try:
        # Get shop names from multiple database fields
        expense_shops = transactions_collection.distinct('shop_name', {'type': 'expense'}) or []
        income_shops = transactions_collection.distinct('category', {'type': 'income'}) or []

        # Combine predefined + db shops
        all_shops = sorted(list(set(SHOP_NAMES + expense_shops + income_shops)))
        return jsonify(all_shops)

    except Exception as e:
        print(f"Error loading shop names: {e}")

        # Fallback: try fetching minimal DB shops
        try:
            expense_shops = transactions_collection.distinct('shop_name') or []
            fallback = sorted(list(set(SHOP_NAMES + expense_shops)))
            return jsonify(fallback)
        except:
            # Final fallback: only predefined
            return jsonify(SHOP_NAMES)


@app.route('/api/profit/monthly-data', methods=['GET'])
@login_required
def get_monthly_profit_data():
    """Get profit data for a specific month"""
    month = request.args.get('month')
    year = request.args.get('year')

    if not month or not year:
        return jsonify({'error': 'Month and year are required'}), 400

    # Calculate start and end dates for the month
    start_date = f"{year}-{month.zfill(2)}-01"
    end_date = f"{year}-{month.zfill(2)}-{get_days_in_month(int(year), int(month))}"

    try:
        # Use aggregation for better performance
        pipeline = [
            {'$match': {'date': {'$gte': start_date, '$lte': end_date}}},
            {'$group': {
                '_id': {'type': '$type', 'category': '$category', 'date': '$date'},
                'amount': {'$sum': '$amount'}
            }}
        ]

        aggregated_data = list(transactions_collection.aggregate(pipeline))
    except:
        aggregated_data = []

    # Process data
    total_income = 0
    total_expense = 0
    income_by_category = defaultdict(float)
    expense_by_category = defaultdict(float)
    daily_breakdown = defaultdict(lambda: {'income': 0, 'expense': 0, 'profit': 0})

    for item in aggregated_data:
        amount = item['amount']
        trans_type = item['_id']['type']
        category = item['_id']['category']
        date = item['_id']['date']

        if trans_type == 'income':
            total_income += amount
            income_by_category[category] += amount
            daily_breakdown[date]['income'] += amount
        else:
            total_expense += amount
            expense_by_category[category] += amount
            daily_breakdown[date]['expense'] += amount

        daily_breakdown[date]['profit'] = daily_breakdown[date]['income'] - daily_breakdown[date]['expense']

    net_profit = total_income - total_expense
    profit_margin = (net_profit / total_income * 100) if total_income > 0 else 0

    # Sort daily breakdown by date
    daily_breakdown = dict(sorted(daily_breakdown.items()))

    # Calculate advanced analytics
    analytics = calculate_advanced_analytics(daily_breakdown)

    return jsonify({
        'month': month,
        'year': year,
        'total_income': total_income,
        'total_expense': total_expense,
        'net_profit': net_profit,
        'profit_margin': profit_margin,
        'income_by_category': dict(income_by_category),
        'expense_by_category': dict(expense_by_category),
        'daily_breakdown': daily_breakdown,
        'analytics': analytics,
        'transaction_count': len(aggregated_data)
    })


@app.route('/api/profit/current-month-summary', methods=['GET'])
@login_required
def get_current_month_summary():
    """Get summary for current month"""
    today = datetime.now()
    month = today.month
    year = today.year

    start_date = f"{year}-{month:02d}-01"
    end_date = f"{year}-{month:02d}-{get_days_in_month(year, month)}"

    try:
        # Use aggregation for better performance
        pipeline = [
            {'$match': {'date': {'$gte': start_date, '$lte': end_date}}},
            {'$group': {
                '_id': '$type',
                'total': {'$sum': '$amount'}
            }}
        ]

        result = list(transactions_collection.aggregate(pipeline))
    except:
        result = []

    total_income = 0
    total_expense = 0

    for item in result:
        if item['_id'] == 'income':
            total_income = item['total']
        elif item['_id'] == 'expense':
            total_expense = item['total']

    net_profit = total_income - total_expense
    profit_margin = (net_profit / total_income * 100) if total_income > 0 else 0

    return jsonify({
        'total_income': total_income,
        'total_expense': total_expense,
        'net_profit': net_profit,
        'profit_margin': profit_margin
    })


@app.route('/api/profit/date-range', methods=['GET'])
@login_required
def get_profit_date_range():
    """Get profit data for date range"""
    start_date = request.args.get('start')
    end_date = request.args.get('end')

    if not start_date or not end_date:
        return jsonify({'error': 'Start and end dates are required'}), 400

    try:
        # Use aggregation for better performance
        pipeline = [
            {'$match': {'date': {'$gte': start_date, '$lte': end_date}}},
            {'$group': {
                '_id': {'type': '$type', 'category': '$category', 'date': '$date'},
                'amount': {'$sum': '$amount'}
            }}
        ]

        aggregated_data = list(transactions_collection.aggregate(pipeline))
    except:
        aggregated_data = []

    # Process data
    total_income = 0
    total_expense = 0
    income_by_category = defaultdict(float)
    expense_by_category = defaultdict(float)
    daily_breakdown = defaultdict(lambda: {'income': 0, 'expense': 0, 'profit': 0})

    for item in aggregated_data:
        amount = item['amount']
        trans_type = item['_id']['type']
        category = item['_id']['category']
        date = item['_id']['date']

        if trans_type == 'income':
            total_income += amount
            income_by_category[category] += amount
            daily_breakdown[date]['income'] += amount
        else:
            total_expense += amount
            expense_by_category[category] += amount
            daily_breakdown[date]['expense'] += amount

        daily_breakdown[date]['profit'] = daily_breakdown[date]['income'] - daily_breakdown[date]['expense']

    net_profit = total_income - total_expense
    profit_margin = (net_profit / total_income * 100) if total_income > 0 else 0

    # Sort daily breakdown by date
    daily_breakdown = dict(sorted(daily_breakdown.items()))

    # Calculate advanced analytics
    analytics = calculate_advanced_analytics(daily_breakdown)

    return jsonify({
        'total_income': total_income,
        'total_expense': total_expense,
        'net_profit': net_profit,
        'profit_margin': profit_margin,
        'daily_breakdown': daily_breakdown,
        'income_by_category': dict(income_by_category),
        'expense_by_category': dict(expense_by_category),
        'analytics': analytics,
        'transaction_count': len(aggregated_data)
    })


def get_days_in_month(year, month):
    """Get the number of days in a month"""
    if month == 12:
        return 31
    next_month = datetime(year, month + 1, 1)
    last_day = next_month - timedelta(days=1)
    return last_day.day


def get_all_dates_in_month(year, month):
    """Get all dates in a month as strings in YYYY-MM-DD format"""
    num_days = get_days_in_month(year, month)
    dates = []
    for day in range(1, num_days + 1):
        date_str = f"{year}-{month:02d}-{day:02d}"
        dates.append(date_str)
    return dates


def open_browser():
    """Open the web browser to the application URL"""
    time.sleep(2)  # Wait for server to start
    try:
        webbrowser.open_new('http://127.0.0.1:8000/')
    except:
        print("Application is running at: http://127.0.0.1:8000/")


if __name__ == '__main__':
    # Create templates directory if it doesn't exist
    if not os.path.exists('templates'):
        os.makedirs('templates')
        print(f"Created templates directory")

    # Start browser in a separate thread
    if os.name == 'nt':  # Only open browser on Windows (for executable)
        browser_thread = threading.Thread(target=open_browser, daemon=True)
        browser_thread.start()

    print("Starting Ice Cream Shop Management System...")
    print("Press Ctrl+C to stop the server")

    # Run the application
    try:
        app.run(debug=False, host='127.0.0.1', port=8000, threaded=True)
    except KeyboardInterrupt:
        print("\nServer stopped by user")
    except Exception as e:
        print(f"Error starting server: {e}")