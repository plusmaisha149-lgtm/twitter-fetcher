#!/usr/bin/env python3
"""
==============================================================================
ENHANCED TWITTER DATA FETCHER - FULLY COMMENTED VERSION
==============================================================================
Purpose: Fetch tweets with FULL text and reply counts from Twitter API
Author: Your Name
Last Updated: January 6, 2026

NEW FEATURES in this version:
1. Gets FULL tweet text (no truncation on retweets)
2. Fetches reply_count (how many comments each tweet has)
3. Stores conversation_id (for tracking discussion threads)
4. Identifies retweets and gets original content
5. FIXED: Handles datetime JSON serialization properly

This script:
1. Connects to Twitter API using Bearer Token
2. Searches for specific keywords (like "Samia Suluhu Hassan")
3. Extracts COMPLETE tweet data (text, likes, retweets, replies, etc.)
4. Stores tweets in PostgreSQL database on Render
5. Runs automatically every 15 minutes via Render Cron Job

==============================================================================
"""

# ==============================================================================
# IMPORT REQUIRED LIBRARIES
# ==============================================================================

import psycopg2      # Library to connect to PostgreSQL database
import json          # Library to handle JSON data (convert Python objects to JSON)
import tweepy        # Library to interact with Twitter API
from datetime import datetime  # Library to work with dates and times
import os            # Library to read environment variables

# ==============================================================================
# CONFIGURATION - CREDENTIALS AND SETTINGS
# ==============================================================================

# DATABASE CONNECTION STRING
# This tells Python how to connect to your PostgreSQL database on Render
# Format: postgresql://username:password@host:port/database_name
DATABASE_URL = os.getenv(
    'DATABASE_URL',
    'postgresql://twitter_data_pygj_user:bBZRdS06QyhBHhBGWRtl4pkLjlaFO5MS@dpg-d5adn26r433s738bha50-a.singapore-postgres.render.com/twitter_data_pygj'
)

# TWITTER API AUTHENTICATION
# This is your Bearer Token that proves you have permission to use Twitter API
# Twitter generates this when you create an app in their Developer Portal
BEARER_TOKEN = os.getenv(
    'TWITTER_BEARER_TOKEN',
    'AAAAAAAAAAAAAAAAAAAAACi4zgEAAAAA0qATv6QklrjGbF8Pe9hdr1mavRk%3DTEwNMv92PRuxVfEbZfHxx098RMIbBzRyOMlNO1lsfXsA1GDr9U'
)

# ==============================================================================
# FUNCTION 1: FETCH TWEETS FROM TWITTER (ENHANCED)
# ==============================================================================

def fetch_tweets():
    """
    This function connects to Twitter API and fetches tweets with FULL TEXT
    
    ENHANCEMENTS in this version:
    - Gets complete text (no truncation on retweets)
    - Fetches reply_count (number of comments)
    - Stores conversation_id (for threading)
    - Identifies if tweet is a retweet
    
    Steps:
    1. Create Twitter API client with authentication
    2. Search for tweets using keywords
    3. Extract ALL available data from each tweet
    4. Get full text for retweets (not truncated)
    5. Store in database with enhanced metadata
    
    Returns:
        None (stores tweets directly to database)
    """
    
    # Print start message with timestamp
    print("[%s] Starting ENHANCED Twitter fetch..." % datetime.now())
    
    try:
        # ------------------------------------------------------------------
        # STEP 1: CREATE TWITTER CLIENT (Authentication)
        # ------------------------------------------------------------------
        
        # Create a connection to Twitter API using our Bearer Token
        # wait_on_rate_limit=True means: if we hit rate limit, wait automatically
        client = tweepy.Client(
            bearer_token=BEARER_TOKEN,
            wait_on_rate_limit=True
        )
        
        print("Connected to Twitter API")
        
        # ------------------------------------------------------------------
        # STEP 2: DEFINE SEARCH QUERIES
        # ------------------------------------------------------------------
        
        # List of keywords/phrases to search for
        # Twitter will find tweets containing these exact phrases
        queries = [
            "Samia Suluhu Hassan",
        ]
        
        # Empty list to store all tweets we find
        all_tweets = []
        
        # ------------------------------------------------------------------
        # STEP 3: SEARCH FOR EACH QUERY
        # ------------------------------------------------------------------
        
        # Loop through each search term
        for query in queries:
            print("Searching: %s" % query)
            
            try:
                # Ask Twitter API to search for recent tweets
                # This is the main API call that fetches data from Twitter
                # NEW! We're requesting MORE fields to get complete data
                response = client.search_recent_tweets(
                    query=query,
                    max_results=10,
                    
                    # TWEET FIELDS: What information about the tweet do we want?
                    tweet_fields=[
                        'created_at',
                        'public_metrics',
                        'lang',
                        'author_id',
                        'conversation_id',
                        'referenced_tweets',
                        'in_reply_to_user_id'
                    ],
                    
                    # EXPANSIONS: Get additional related data
                    expansions=[
                        'author_id',
                        'referenced_tweets.id'
                    ],
                    
                    # USER FIELDS: What information about the author do we want?
                    user_fields=[
                        'username',
                        'name',
                        'verified',
                        'public_metrics'
                    ]
                )
                
                # ------------------------------------------------------------------
                # STEP 4: CHECK IF WE GOT ANY RESULTS
                # ------------------------------------------------------------------
                
                if not response.data:
                    print("  No tweets found for '%s'" % query)
                    continue
                
                # ------------------------------------------------------------------
                # STEP 5: BUILD LOOKUP TABLES
                # ------------------------------------------------------------------
                
                # --- USER LOOKUP TABLE ---
                users = {}
                if response.includes and 'users' in response.includes:
                    users = {u.id: u for u in response.includes['users']}
                
                # --- REFERENCED TWEETS LOOKUP TABLE (NEW!) ---
                # For retweets, Twitter returns the ORIGINAL tweet separately
                # We need this to get the FULL TEXT (not truncated)
                referenced = {}
                if response.includes and 'tweets' in response.includes:
                    referenced = {t.id: t for t in response.includes['tweets']}
                
                # ------------------------------------------------------------------
                # STEP 6: PROCESS EACH TWEET
                # ------------------------------------------------------------------
                
                # Loop through each tweet we found
                for tweet in response.data:
                    
                    # Get Author Information
                    author = users.get(tweet.author_id, None)
                    if author:
                        username = author.username
                    else:
                        username = "user_%s" % tweet.author_id
                    
                    # ------------------------------------------------------------------
                    # NEW! GET FULL TEXT (Handle Retweets Properly)
                    # ------------------------------------------------------------------
                    
                    full_text = tweet.text
                    is_retweet = False
                    original_tweet_id = None
                    
                    # Check if this tweet references another tweet
                    if tweet.referenced_tweets:
                        for ref in tweet.referenced_tweets:
                            if ref.type == 'retweeted':
                                is_retweet = True
                                original_tweet_id = ref.id
                                
                                # Get the FULL TEXT from the original tweet
                                if ref.id in referenced:
                                    original = referenced[ref.id]
                                    full_text = "RT @%s: %s" % (username, original.text)
                    
                    # Extract reply count
                    reply_count = tweet.public_metrics.get('reply_count', 0)
                    
                    # Extract quote count
                    quote_count = tweet.public_metrics.get('quote_count', 0)
                    
                    # Build Tweet Data Dictionary
                    tweet_data = {
                        'id': tweet.id,
                        'text': full_text,
                        'created_at': tweet.created_at,
                        'author_username': username,
                        'retweet_count': tweet.public_metrics['retweet_count'],
                        'like_count': tweet.public_metrics['like_count'],
                        'reply_count': reply_count,
                        'quote_count': quote_count,
                        'conversation_id': tweet.conversation_id,
                        'is_retweet': is_retweet,
                        'original_tweet_id': original_tweet_id,
                        'raw_data': {
                            'query': query,
                            'language': tweet.lang,
                            'is_mock': False,
                            'source': 'twitter_api_enhanced'
                        }
                    }
                    
                    all_tweets.append(tweet_data)
                
                print("  Found %d tweets (with FULL text)" % len(response.data))
                
            except Exception as e:
                print("  Error searching '%s': %s" % (query, e))
                continue
        
        # ------------------------------------------------------------------
        # STEP 7: PRINT SUMMARY
        # ------------------------------------------------------------------
        
        print("")
        print("Total collected: %d tweets" % len(all_tweets))
        
        # ------------------------------------------------------------------
        # STEP 8: STORE TWEETS IN DATABASE
        # ------------------------------------------------------------------
        
        if all_tweets:
            store_tweets(all_tweets)
        else:
            print("No tweets to store")
        
        print("[%s] Fetch completed!" % datetime.now())
        print("")
        
    except Exception as e:
        print("Fatal error: %s" % e)

# ==============================================================================
# FUNCTION 2: STORE TWEETS IN DATABASE (ENHANCED + FIXED)
# ==============================================================================

def store_tweets(tweets):
    """
    This function saves tweets to PostgreSQL database
    
    ENHANCEMENT: Now uses UPDATE on conflict to refresh data
    FIX: Converts datetime objects to strings for JSON storage
    
    Args:
        tweets (list): List of tweet dictionaries to save
        
    Process:
    1. Connect to PostgreSQL database
    2. For each tweet, convert datetime to string for JSON
    3. Try to insert tweet, or UPDATE if already exists
    4. Commit changes and close connection
    
    Returns:
        None (prints success/error messages)
    """
    
    try:
        # Connect to database
        print("Storing tweets in database...")
        
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        
        stored_count = 0
        
        # Loop through each tweet
        for tweet in tweets:
            try:
                # ------------------------------------------------------------------
                # FIX: Prepare tweet data for JSON storage
                # ------------------------------------------------------------------
                
                # Create a copy and convert datetime to ISO string
                tweet_for_json = tweet.copy()
                
                # Convert datetime object to ISO format string
                if isinstance(tweet_for_json['created_at'], datetime):
                    tweet_for_json['created_at'] = tweet_for_json['created_at'].isoformat()
                
                # Insert or update tweet
                cursor.execute("""
                    INSERT INTO tweets (
                        id,
                        text,
                        author_username,
                        created_at,
                        retweet_count,
                        like_count,
                        raw_data
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO UPDATE SET
                        text = EXCLUDED.text,
                        retweet_count = EXCLUDED.retweet_count,
                        like_count = EXCLUDED.like_count,
                        raw_data = EXCLUDED.raw_data
                """, (
                    tweet['id'],
                    tweet['text'],
                    tweet['author_username'],
                    tweet['created_at'],
                    tweet['retweet_count'],
                    tweet['like_count'],
                    json.dumps(tweet_for_json)
                ))
                
                if cursor.rowcount > 0:
                    stored_count += 1
                
            except Exception as e:
                print("Error storing tweet %s: %s" % (tweet['id'], e))
                continue
        
        # Save changes
        conn.commit()
        cursor.close()
        conn.close()
        
        print("Stored/Updated: %d tweets with FULL text and reply counts" % stored_count)
        
    except Exception as e:
        print("Database error: %s" % e)

# ==============================================================================
# MAIN PROGRAM ENTRY POINT
# ==============================================================================

if __name__ == "__main__":
    """
    This code runs when the script is executed directly
    """
    
    try:
        fetch_tweets()
        
    except KeyboardInterrupt:
        print("")
        print("")
        print("Interrupted by user")
        
    except Exception as e:
        print("")
        print("")
        print("Unexpected error: %s" % e)

# ==============================================================================
# END OF SCRIPT
# ==============================================================================
