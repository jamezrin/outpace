# Synthetic original-disk envelope vector

`disk-envelope-unsorted.acestream` is a deterministic AES-128-CBC reference encryption of the adjacent `single-file-unsorted.torrent` synthetic fixture, using the documented exact marker, fixed protocol key, zero IV, and OpenSSL's standard PKCS#7 padding. It contains no live content identifier, infohash, or stream name. The production decoder is not used to create this vector.
