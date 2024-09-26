.. image:: snackabra.svg
   :height: 100px
   :align: center
   :alt: The 'michat' Pet Logo

=====================
Snackabra Room Server
=====================

_This is branch 'psm-refactor' off a fork of snackabra/snackabra-channelserver.
 In os384 this is superceded by 'servers-2024-rcN'. Do NOT develop here further_

This repository is for the chat room server. For general documentation
on Snackabra see: https://snackabra.io

The room server allows you to host and manage your own Snackabra
server, called a "personal" server (as opposed to a "public"
server). This same code supports a public (multi-user) Snackabra
service, but that requires other building blocks (including you need
an SSO), so for now this documentation will focus on setting up a
personal server. On a personal server, the individual running the
server is de-facto "owner" of all rooms - see the general
documentation for more information.

You will need a client to connect to the server. We currently have two
reference implementations, a web app version and an iOS version:

* https://snackabra.github.com/snackabra-webclient

* https://snackabra.github.com/snackabra-ios (early version)

In the clients you can then point to your server (once it's up and
running).


Setup (Personal Room Server)
----------------------------

The current room server requires a domain name and a Cloudflare (CF)
account. Currently, a free CF account is _almost_ sufficient, but
"durable objects" are not available yet on the free plans, so that
sets a minimum of $5/month to host a personal server (*).

If you want (push) notifications for mobile you will need to set up
the necessary accounts with Apple; details are beyond this
documentation.

* Set up a domain (we will call it "example.com") that you control.
  You will need to be able to change the nameservers to be Cloudflare.

* Set up a free account with CF: https://dash.cloudflare.com/sign-up -
  use your domain in the signup process.

* Go to the "workers" section and pick a name for your worker on
  CF, we'll call it "example" here. That sets up a subdomain on
  "workers.dev", e.g. "example.workers.dev."  Later you can set
  up "routes" from own domain.

* Click on the "Free" button, you need to upgrade to the
  "Pay-as-you-go" plan.

Now you have the account(s) set up. You might need to check email for
when any nameservers have propagated.

Next set up the CF command line environment, the "Wrangler CLI", we
use "yarn" in general but the personal server code is pure JS and
(currently) does not need any node packages. Follow instructions at
https://developers.cloudflare.com/workers/cli-wrangler/ -
at time of writing:

::

   # install the CLI:
   yarn global add @cloudflare/wrangler
   # authenticate your CLI:
   wrangler login
   # copy the template 'toml' file
   cp setup/template.wranger.toml wrangler.toml

The 'login' will open a web page to confirm that your CLI is allowed
to administrate your CF account.

(Note that we are using wrangler 3.0 at this time)

In the above 'wrangler.toml' file, you will need to add your 'Account
ID' from the dashboard. Next, you will need a few "KV Namespaces". You
can do that with the CLI:

::

   wrangler kv:namespace create "MESSAGES_NAMESPACE"
   wrangler kv:namespace create "KEYS_NAMESPACE"
   wrangler kv:namespace create "LEDGER_NAMESPACE"

For each of them, you need to copy-paste the corresponding 'id' to
your ```wrangler.toml``` file.

Before you deploy, you need to enable "Durable Objects" for your
account.  On your "Workers" dashboard there is currently a link
"Durable Objects is now generally available!" - click that.(**)

Finally, you need to make a tiny change to your copy of
the server code, providing a 'secret'. This is essentially a simple
auth token that your server will request every time you create a new
room, or migrate a room over from somewhere else.

::

   wrangler secret put SERVER_SECRET<enter>

It will prompt you to enter the secret.

You also need to generate a RSA-OAEP key for the ledger, in 'jwk' format.
This is used for internal communication of storage allocation approvals.

There are a few ways to do that. If you are running Node v15 or higher
the mint_keys.js script will mint and store them locally:

```
node ./mint_keys.js
```

Or if you want to be more secure (so that the private key never
touches your file system), you can for, example open any browser
console and enter the following Javascript:

::
   let keyPair = await window.crypto.subtle.generateKey(
     {
       name: "RSA-OAEP",
       modulusLength: 4096,
       publicExponent: new Uint8Array([1, 0, 1]),
       hash: "SHA-256"
     },
     true,
     ["encrypt", "decrypt"]
   );
   let my_private_key = await window.crypto.subtle.exportKey("jwk", keyPair.privateKey);
   let my_public_key = await window.crypto.subtle.exportKey("jwk", keyPair.publicKey);
   JSON.stringify(my_public_key);

(See https://developer.mozilla.org/en-US/docs/Web/API/SubtleCrypto/generateKey#rsa_key_pair_generation)

And you should get something like:

::

   '{"alg":"RSA-OAEP-256","e":"AQAB","ext":true,"key_ops":["encrypt"],"kty":"RSA","n":"mOmu ....


(Don't forget to store the full / private key somewhere secure: ``JSON.stringify(my_private_key)``)

The resulting string (include the quotes) is the string you enter as your "LEDGER_KEY":

::

   wrangler secret put LEDGER_KEY<enter>

Now you should be able to start your server:

::

   wrangler publish

And point a client to it!


(*) We are not affiliated with Cloudflare, we're just fans.

Log into the Cloudflare dashboard > Workers > Durable Objects


Setup (Development)
-------------------

If you are developing locally, we suggest https://miniflare.dev/

Setup is similar as above:

```
# copy the template 'toml' file for miniflare
cp setup/miniflare.wrangler.toml wrangler.toml
```

The only thing you need to change is the "LEDGER_KEY" (to the generated public key).

Then setup packages and run:

```
npm install
npm run miniflare
```

It should fire up on ```http://127.0.0.1:4001```





LICENSE
-------

Copyright (c) 2016-2023 Magnusson Institute, All Rights Reserved.

"Snackabra" is a registered trademark

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as
published by the Free Software Foundation, either version 3 of the
License, or (at your option) any later version.

This program is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
Affero General Public License for more details.

Licensed under GNU Affero General Public License
https://www.gnu.org/licenses/agpl-3.0.html


Cryptography Notice
-------------------

This distribution includes cryptographic software. The country in
which you currently reside may have restrictions on the import,
possession, use, and/or re-export to another country, of encryption
software. Before using any encryption software, please check your
country's laws, regulations and policies concerning the import,
possession, or use, and re-export of encryption software, to see if
this is permitted. See http://www.wassenaar.org/ for more information.

United States: This distribution employs only "standard cryptography"
under BIS definitions, and falls under the Technology Software
Unrestricted (TSU) exception.  Futher, per the March 29, 2021,
amendment by the Bureau of Industry & Security (BIS) amendment of the
Export Administration Regulations (EAR), this "mass market"
distribution does not require reporting (see
https://www.govinfo.gov/content/pkg/FR-2021-03-29/pdf/2021-05481.pdf ).
