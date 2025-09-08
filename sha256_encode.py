# import hashlib
#
# # Input string
# input_string = "G-572137558"
#
# # Function to convert UTF-8 string to SHA-256 hex
# def utf8_to_sha256_hex(input_string):
#     encoded_input = input_string.encode('utf-8')
#     sha256_hash = hashlib.sha256(encoded_input).hexdigest()
#     return sha256_hash
#
# # Get the SHA-256 hash in hexadecimal
# hash_hex = utf8_to_sha256_hex(input_string)
# print(f"SHA-256 Hash in Hex: {hash_hex}")

microseconds = 1743841825000000  # your microsecond value
byte_string = microseconds.to_bytes(8, byteorder='big', signed=False)
escaped_hex = ''.join(f'\\x{b:02x}' for b in byte_string)
print(escaped_hex)
