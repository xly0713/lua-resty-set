local ffi = require "ffi"

local ffi_new = ffi.new
local ffi_str = ffi.string
local C = ffi.C

local _M = { _VERSION = "0.01" }

ffi.cdef[[
typedef struct rc4_key_st
    {
        unsigned int x, y;
        unsigned int data[256];
    } RC4_KEY;

void RC4_set_key(RC4_KEY *key, int len, const unsigned char *data);
void RC4(RC4_KEY *key, size_t len, const unsigned char *indata, unsigned char *outdata);
]]

function _M.crypt(plaintext, key)
    local len = #plaintext
    local ciphertext = ffi_new("unsigned char[?]", len)
    local rc4_key = ffi_new(ffi.typeof("RC4_KEY[1]"))

    C.RC4_set_key(rc4_key, #key, key);
    C.RC4(rc4_key, len, plaintext, ciphertext)

    return ffi_str(ciphertext, len)
end

return _M
