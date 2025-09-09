# PreorderUploadScriptForCoverlandShopify

1. How to Setup

   - git clone: https://github.com/BK-Coverland/PreorderUploadScriptForCoverlandShopify.git
  
   - Install dependencies: pip install -r requirements.txt
  
    
2. ENV file Create

# Initialize Shopify GraphQL API
SHOPIFY_ENDPOINT="https://aaa.myshopify.com/admin/api/0000-00/graphql.json"
SHOPIFY_ACCESS_TOKEN="shpat_xxxxx"


# Initialize Supabase client
SUPABASE_URL = 'https://xxx.supabase.co'
SUPABASE_ANON_KEY = 'xxxx'


# File Location
PREORDER_TARGET_CSV = "C:/{path}/Target CSV/"
PREORDER_SOURCE_CSV = "C:/{path}/Source CSV/"

PREORDER_OFFER_TABLE = "table1"
PREORDER_VARIANT_TABLE = "table2"
PREORDER_BATCH_OFFER_TABLE = "table3"
PREORDER_BATCH_VARIANT_TABLE = "table4"

# Constant Values
SOURCE_FILE_NAME_LIST = ""
SUFFIX_LIST = ""
SHOPIFY_DELIVERY_PROFILE_ID = ""



3. Run the script:

python run_all.py

4. General, set venv is highly recommended.

   
