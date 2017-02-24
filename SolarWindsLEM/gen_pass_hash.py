# Author: Chris Graham
import hashlib
import struct
import getpass

# Custom base64 implementation which is derived from Base64.class in
# lem_util.jar. 
def base64_encode(source):
    alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+-"
    encode = []
    n = ['\x00', '\x00', '\x00', '\x00']
    padding = 3 - len(source) % 3
    if padding == 3:
        padding = 0
    encode.append(alphabet[padding])
    for i in range(0, len(source) + padding):
        if i < len(source):
            n[(i%3)] = source[i]
        else:
            n[(i%3)] = '\x00'
        if(i % 3 == 2):
            temp = (int(struct.unpack('b', n[2])[0]) & 0xff) + ((int(struct.unpack('b', n[1])[0]) & 0xff) << 8) + ((int(struct.unpack('b', n[0])[0]) & 0xff) << 16)
            n[0] = (temp >> 18 & 0x3f)
            n[1] = (temp >> 12 & 0x3f)
            n[2] = (temp >> 6 & 0x3f)
            n[3] = (temp & 0x3f)
            encode.append(alphabet[n[0]])
            encode.append(alphabet[n[1]])
            encode.append(alphabet[n[2]])
            encode.append(alphabet[n[3]])
    return ''.join([str(i) for i in encode])

if __name__ == "__main__":
    md = hashlib.sha1()
    user = raw_input("New username: ")
    passwd = getpass.getpass("Password: ")
    passwd_again = getpass.getpass("Password (re-enter): ")
    if passwd != passwd_again:
        print "Passwords do not match!"
        exit()
    else:
        md.update(user + passwd)
        print "New user hash: " + base64_encode(md.digest())
