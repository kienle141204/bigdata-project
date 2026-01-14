"""Utility script to manage Kafka topics."""
import sys
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from loguru import logger

from kafka.config import KAFKA_CONFIG, TOPICS, TOPIC_CONFIGS


def create_topics(bootstrap_servers: str = None):
    """
    Create all required Kafka topics.
    
    Args:
        bootstrap_servers: Kafka broker address
    """
    bootstrap_servers = bootstrap_servers or KAFKA_CONFIG["bootstrap_servers"]
    
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            client_id="topic_manager"
        )
        
        logger.info(f"Connected to Kafka: {bootstrap_servers}")
        
        # Create topic objects
        topics_to_create = []
        for topic_name, config in TOPIC_CONFIGS.items():
            new_topic = NewTopic(
                name=topic_name,
                num_partitions=config["num_partitions"],
                replication_factor=config["replication_factor"],
                topic_configs=config.get("config", {})
            )
            topics_to_create.append(new_topic)
        
        # Create topics
        logger.info(f"Creating {len(topics_to_create)} topics...")
        
        try:
            admin_client.create_topics(new_topics=topics_to_create, validate_only=False)
            logger.success(f"✅ Successfully created topics:")
            for topic in topics_to_create:
                logger.info(f"   - {topic.name} (partitions: {topic.num_partitions})")
        
        except TopicAlreadyExistsError:
            logger.warning("⚠️  Some topics already exist")
        
        # List all topics
        existing_topics = admin_client.list_topics()
        logger.info(f"\n📋 All topics in cluster: {existing_topics}")
        
        admin_client.close()
        
    except Exception as e:
        logger.error(f"❌ Error creating topics: {e}")
        sys.exit(1)


def delete_topics(bootstrap_servers: str = None):
    """
    Delete all project-related topics.
    
    Args:
        bootstrap_servers: Kafka broker address
    """
    bootstrap_servers = bootstrap_servers or KAFKA_CONFIG["bootstrap_servers"]
    
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            client_id="topic_manager"
        )
        
        topics_to_delete = list(TOPICS.values())
        
        logger.warning(f"⚠️  Deleting {len(topics_to_delete)} topics: {topics_to_delete}")
        admin_client.delete_topics(topics=topics_to_delete, timeout_ms=5000)
        logger.success("✅ Topics deleted")
        
        admin_client.close()
        
    except Exception as e:
        logger.error(f"❌ Error deleting topics: {e}")


def list_topics(bootstrap_servers: str = None):
    """
    List all Kafka topics.
    
    Args:
        bootstrap_servers: Kafka broker address
    """
    bootstrap_servers = bootstrap_servers or KAFKA_CONFIG["bootstrap_servers"]
    
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            client_id="topic_manager"
        )
        
        topics = admin_client.list_topics()
        logger.info(f"📋 Kafka Topics ({len(topics)}):")
        for topic in sorted(topics):
            logger.info(f"   - {topic}")
        
        admin_client.close()
        
    except Exception as e:
        logger.error(f"❌ Error listing topics: {e}")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Kafka Topic Management")
    parser.add_argument("action", choices=["create", "delete", "list"], help="Action to perform")
    parser.add_argument("--servers", type=str, help="Kafka bootstrap servers")
    
    args = parser.parse_args()
    
    if args.action == "create":
        create_topics(args.servers)
    elif args.action == "delete":
        delete_topics(args.servers)
    elif args.action == "list":
        list_topics(args.servers)
