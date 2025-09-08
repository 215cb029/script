from datetime import datetime

s =b'\x00\x0662\x90\xfc\x91K'
value = int.from_bytes(s, byteorder='big', signed=False)

print(value)
#1687163425000000
#1741457165000001