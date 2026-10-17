import os
import sys
import json
import logging
import pandas as pd

# Add project root to path
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, PROJECT_ROOT)

if sys.platform.startswith('win'):
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except Exception:
        pass

from utils.kafka_utils import NativeKafkaConsumer, NativeKafkaConfig

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    logger.info("Initializing Kafka Analytics Consumer...")
    
    # Load config and topic info
    config = NativeKafkaConfig()
    topic = config.topics.get('fraud_predictions', 'fraud_predictions')
    
    # Create unique consumer group to always read from earliest offset
    consumer_group = f"fraud-analytics-{pd.Timestamp.now().value}"
    
    consumer = NativeKafkaConsumer(group_id=consumer_group, topics=[topic])
    logger.info(f"Connected to topic '{topic}'. Fetching scored transactions for analysis...")
    
    # Poll messages from early offset
    # Poll up to 2000 messages with 10 second timeout
    df = consumer.consume_to_dataframe(max_messages=2000, timeout=10)
    
    if df.empty:
        logger.warning(f"No messages found in topic '{topic}'. Make sure the consumer pipeline is running and scoring events.")
        consumer.close()
        return
        
    logger.info(f"Successfully retrieved {len(df)} scored prediction events.")
    
    # Parse payload
    transactions = []
    predictions = []
    probabilities = []
    timestamps = []
    
    for idx, row in df.iterrows():
        try:
            pred_data = row.get('prediction', {})
            trans_data = row.get('original_data', row.get('transaction', {}))
            
            if isinstance(pred_data, str):
                try:
                    pred_data = json.loads(pred_data)
                except Exception:
                    pass
            if isinstance(trans_data, str):
                try:
                    trans_data = json.loads(trans_data)
                except Exception:
                    pass

            if isinstance(pred_data, dict):
                pred_label = int(pred_data.get('Prediction', pred_data.get('is_fraud', 0)))
                pred_prob = float(pred_data.get('Probability', pred_data.get('fraud_probability', 0.0)))
                timestamp = row.get('processed_at', pred_data.get('scored_at', ''))

                transactions.append(trans_data if isinstance(trans_data, dict) else {})
                predictions.append(pred_label)
                probabilities.append(pred_prob)
                timestamps.append(timestamp)
        except Exception as e:
            continue
            
    if not predictions:
        logger.warning("Could not parse prediction records from topic messages.")
        consumer.close()
        return
        
    total_count = len(predictions)
    fraud_count = sum(predictions)
    legit_count = total_count - fraud_count
    avg_prob = sum(probabilities) / total_count
    max_prob = max(probabilities)
    fraud_rate = (fraud_count / total_count) * 100
    
    print("\n" + "="*50)
    print("      [METRICS] KAFKA REAL-TIME ML PREDICTION ANALYTICS")
    print("="*50)
    print(f"Total Transactions Scored : {total_count}")
    print(f"Legitimate Transactions   : {legit_count} ({(legit_count/total_count)*100:.2f}%)")
    print(f"Fraud Transactions (Alert): {fraud_count} ({fraud_rate:.2f}%)")
    print(f"Average Fraud Probability : {avg_prob:.4f}")
    print(f"Maximum Fraud Probability : {max_prob:.4f}")
    print("="*50)
    
    # Show top 5 highest fraud risk transactions
    scored_df = pd.DataFrame({
        'cc_num': [t.get('cc_num', '') for t in transactions],
        'amount': [t.get('amt', 0.0) for t in transactions],
        'merchant': [t.get('category', '') for t in transactions],
        'probability': probabilities,
        'prediction': predictions
    })
    
    high_risk = scored_df.sort_values(by='probability', ascending=False).head(5)
    print("\n[HIGH RISK] TOP 5 HIGHEST FRAUD RISK TRANSACTIONS:")
    print("-" * 75)
    print(f"{'Card Number':<18} | {'Category':<15} | {'Amount':<10} | {'Probability':<12} | {'Status':<10}")
    print("-" * 75)
    for _, row in high_risk.iterrows():
        status = "[FRAUD]" if row['prediction'] == 1 else "[CLEAN]"
        print(f"{str(row['cc_num'])[:16]:<18} | {row['merchant']:<15} | ${row['amount']:<9.2f} | {row['probability']:<12.4f} | {status:<10}")
    print("-" * 75)
    print("\n")
    
    consumer.close()

if __name__ == "__main__":
    import json
    main()
