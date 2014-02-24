# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from __future__ import unicode_literals

from ..cnv import CNV
from ..env.logs import Log
from ..queries import Q
from ..struct import Struct
from ..maths.randoms import Random
from ..vendor.aespython import key_expander, aes_cipher, cbc_mode


DEBUG = False


def encrypt(text, _key, salt=None):
    """
    RETURN JSON OF ENCRYPTED DATA   {"salt":s, "length":l, "data":d}
    """
    if not isinstance(text, unicode):
        Log.error("only unicode is encrypted")
    if _key is None:
        Log.error("Expecting a key")

    if salt is None:
        salt = Random.bytes(16)

    data = bytearray(text.encode("utf8"))

    #Initialize encryption using key and iv
    key_expander_256 = key_expander.KeyExpander(256)
    expanded_key = key_expander_256.expand(_key)
    aes_cipher_256 = aes_cipher.AESCipher(expanded_key)
    aes_cbc_256 = cbc_mode.CBCMode(aes_cipher_256, 16)
    aes_cbc_256.set_iv(salt)

    output = Struct()
    output.type = "AES256"
    output.salt = CNV.bytearray2base64(salt)
    output.length = len(data)

    encrypted = bytearray()
    for i, d in Q.groupby(data, size=16):
        encrypted.extend(aes_cbc_256.encrypt_block(d))
    output.data = CNV.bytearray2base64(encrypted)
    json = CNV.object2JSON(output)

    if DEBUG:
        test = decrypt(json, _key)
        if test != text:
            Log.error("problem with encryption")

    return json


def decrypt(data, _key):
    """
    ACCEPT JSON OF ENCRYPTED DATA  {"salt":s, "length":l, "data":d}
    """
    #Key and iv have not been generated or provided, bail out
    if _key is None:
        Log.error("Expecting a key")

    _input = CNV.JSON2object(data)

    #Initialize encryption using key and iv
    key_expander_256 = key_expander.KeyExpander(256)
    expanded_key = key_expander_256.expand(_key)
    aes_cipher_256 = aes_cipher.AESCipher(expanded_key)
    aes_cbc_256 = cbc_mode.CBCMode(aes_cipher_256, 16)
    aes_cbc_256.set_iv(CNV.base642bytearray(_input.salt))

    raw = CNV.base642bytearray(_input.data)
    out_data = bytearray()
    for i, e in Q.groupby(raw, size=16):
        out_data.extend(aes_cbc_256.decrypt_block(e))

    return str(out_data[:_input.length:]).decode("utf8")




