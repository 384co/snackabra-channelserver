let crypto;
const fs = require('fs/promises')
try {
    crypto = require('crypto');
} catch (err) {
    console.log('crypto support is disabled!');
}

(async () => {
    let keyPair = await crypto.webcrypto.subtle.generateKey(
        {
            name: "RSA-OAEP",
            modulusLength: 4096,
            publicExponent: new Uint8Array([1, 0, 1]),
            hash: "SHA-256"
        },
        true,
        ["encrypt", "decrypt"]
    );
    let my_private_key = await crypto.webcrypto.subtle.exportKey("jwk", keyPair.privateKey);
    let my_public_key = await crypto.webcrypto.subtle.exportKey("jwk", keyPair.publicKey);
    await fs.writeFile('./my_public_key', JSON.stringify(my_public_key));
    await fs.writeFile('./my_private_key', JSON.stringify(my_private_key));
})()