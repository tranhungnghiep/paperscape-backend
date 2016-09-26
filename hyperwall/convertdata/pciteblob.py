"""Functions for building pcite reference/citation blobs."""

def appendLE16(b, i):
    """Append little-endian 16 bit unsigned integer to bytearray."""
    if i < 0 or i > 0xffff:
        raise Exception("Value %d too large for 16 bits" % i)
    b.append((i >> 0) & 0xff)
    b.append((i >> 8) & 0xff)

def appendLE32(b, i):
    """Append little-endian 32 bit unsigned integer to bytearray."""
    if i < 0 or i > 0xffffffff:
        raise Exception("Value %d too large for 32 bits" % i)
    b.append((i >> 0) & 0xff)
    b.append((i >> 8) & 0xff)
    b.append((i >> 16) & 0xff)
    b.append((i >> 24) & 0xff)

def buildBlob(iter):
    """Encode a list of (id,numCites) into a blob, using fixed little-endian encoding."""
    b = bytearray()
    for id, numCites in iter:
        appendLE32(b, id)
        appendLE16(b, numCites)
    return b

def decodeLE16(b, i):
    """From blob b's offset i, decode a little-endian 16 bit unsigned integer."""
    return b[i + 0] | (b[i + 1]) << 8

def decodeLE32(b, i):
    """From blob b's offset i, decode a little-endian 32 bit unsigned integer."""
    return b[i + 0] | (b[i + 1]) << 8 | (b[i + 2]) << 16 | (b[i + 3]) << 24

def decodeBlob(b):
    """Decode a little-endian blob into a list of tuples of the form (id,numCites)."""
    i = 0
    lst = []
    base_len = 6
    while i < len(b):
        if i + base_len - 1 >= len(b): 
            raise Exception("Pcite blob does not have length a multiple of {}.".format(base_len))
        lst.append((decodeLE32(b, i + 0), decodeLE16(b, i + 4)))
        i += base_len
    return lst
