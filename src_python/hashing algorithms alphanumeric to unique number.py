# Databricks notebook source
# MAGIC %md ## Hashing algorithms
# MAGIC * `MD5 (Message Digest Algorithm 5)`
# MAGIC * `SHA-1 (Secure Hash Algorithm 1)`
# MAGIC * `SHA-2 (Secure Hash Algorithm 2)` SHA-224, SHA-256, SHA-384, SHA-512
# MAGIC
# MAGIC `Note:` MD5 and SHA-1 are considered insecure for cryptographic purposes due to vulnerabilities. For security-sensitive applications, it's recommended to use one of the SHA-2 or SHA-3 family hashes.

# COMMAND ----------

# DBTITLE 1,Option 1 - MD5 (Message Digest Algorithm 5)
import hashlib

def alphanumeric_to_unique_number(alphanumeric_string):
    hash_object = hashlib.md5()
    hash_object.update(alphanumeric_string.encode('utf-8'))
    hash_decimal = int(hash_object.hexdigest(), 16)
    return hash_decimal


list_alphanumeric = ['6oyabdeb803ddf0421c8b9cc792ee505','6oyabdeb803ddf0421c8b9cc792ee505','6pr059137bce30041abb5ff7a05d43f6','6pr059137bce30041abb5ff7a05d43f6','6pr059137bce30041abb5ff7a05d43f6']
for string in list_alphanumeric:
    hash_decimal = alphanumeric_to_unique_number(string)
    print(f'{string} : {hash_decimal}')

# COMMAND ----------

# DBTITLE 1,Option 2 - SHA-2 (Secure Hash Algorithm 2) - sha1
import hashlib

def alphanumeric_to_unique_number(alphanumeric_string):
    hash_object = hashlib.sha1()
    hash_object.update(alphanumeric_string.encode('utf-8'))
    hash_decimal = int(hash_object.hexdigest(), 16)
    return hash_decimal


list_alphanumeric = ['6oyabdeb803ddf0421c8b9cc792ee505','6oyabdeb803ddf0421c8b9cc792ee505','6pr059137bce30041abb5ff7a05d43f6','6pr059137bce30041abb5ff7a05d43f6','6pr059137bce30041abb5ff7a05d43f6']
for string in list_alphanumeric:
    hash_decimal = alphanumeric_to_unique_number(string)
    print(f'{string} : {hash_decimal}')

# COMMAND ----------

# DBTITLE 1,Option 3 - SHA-2 (Secure Hash Algorithm 2) - sha256
import hashlib

def alphanumeric_to_unique_number(alphanumeric_string):
    hash_object = hashlib.sha256()
    hash_object.update(alphanumeric_string.encode('utf-8'))
    hash_decimal = int(hash_object.hexdigest(), 16)
    return hash_decimal


list_alphanumeric = ['6oyabdeb803ddf0421c8b9cc792ee505','6oyabdeb803ddf0421c8b9cc792ee505','6pr059137bce30041abb5ff7a05d43f6','6pr059137bce30041abb5ff7a05d43f6','6pr059137bce30041abb5ff7a05d43f6']
for string in list_alphanumeric:
    hash_decimal = alphanumeric_to_unique_number(string)
    print(f'{string} : {hash_decimal}')
