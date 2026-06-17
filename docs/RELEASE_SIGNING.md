# Package Release Signing Guide

This document describes how to configure GPG signing for KEEL package releases.

## Overview

The `package-linux.yml` workflow automatically signs release artifacts (DEB, RPM, TGZ) with detached `.asc` signatures and generates a SHA256SUMS manifest when the following GitHub repository secrets are configured.

## Prerequisites

- GPG 2.x installed locally
- Admin access to the GitHub repository settings
- An existing GPG key or willingness to generate one

## Step 1: Generate or Prepare a GPG Key

### Option A: Generate a new signing key

```bash
gpg --generate-key
```

Follow the interactive prompts:
- Key type: RSA (default)
- Key size: 4096 bits (recommended)
- Real name: `KEEL Release Signing` (or similar)
- Email: `releases@your-domain.io` (or similar)
- Passphrase: Choose a strong passphrase (this will be used in CI/CD)

### Option B: Use an existing key

List your keys to find the key ID:
```bash
gpg --list-secret-keys --keyid-format LONG
```

## Step 2: Export the Private Key

Export your GPG private key in armored format:

```bash
gpg --armor --export-secret-key <KEY_ID> > signing-key.asc
```

**Important:** Keep this file secure. It will be used to sign all release packages.

Replace `<KEY_ID>` with the long key ID from `gpg --list-secret-keys`.

Example output format:
```
-----BEGIN PGP PRIVATE KEY BLOCK-----
...base64-encoded key data...
-----END PGP PRIVATE KEY BLOCK-----
```

## Step 3: Configure GitHub Secrets

1. Go to your repository on GitHub
2. Navigate to **Settings** → **Secrets and variables** → **Actions**
3. Click **"New repository secret"** and create the following two secrets:

### Secret 1: `PACKAGE_SIGNING_PRIVATE_KEY`

- **Name:** `PACKAGE_SIGNING_PRIVATE_KEY`
- **Value:** Paste the entire contents of `signing-key.asc` (including the `-----BEGIN` and `-----END` lines)

### Secret 2: `PACKAGE_SIGNING_PASSPHRASE`

- **Name:** `PACKAGE_SIGNING_PASSPHRASE`
- **Value:** The passphrase you entered when generating the GPG key

## Step 4: Verify Configuration

Once configured, trigger a release tag:

```bash
git tag -a v0.5.5-alpha -m "Release v0.5.5-alpha"
git push origin v0.5.5-alpha
```

Monitor the workflow at: **Actions** → **package-linux** → Latest run

### Expected output

When signing is configured, you should see:
- ✅ Generate CycloneDX SBOM
- ✅ Import GPG key for package signing
- ✅ Sign release artifacts
- ✅ Upload signatures to Release assets

Release assets will include:
- `keel-*.deb` + `keel-*.deb.asc` (signature)
- `keel-*.rpm` + `keel-*.rpm.asc` (signature)
- `keel-*-Linux-*.tar.gz` + `keel-*-Linux-*.tar.gz.asc` (signature)
- `SHA256SUMS` + `SHA256SUMS.asc` (manifest signature)
- `keel-*.cdx.json` (CycloneDX SBOM)

## Step 5: Verify Signatures (End Users)

Users can verify the authenticity of released packages:

```bash
# Import your public key (if not already in their keyring)
gpg --recv-keys <KEY_ID>

# Verify a package signature
gpg --verify keel-core-0.5.5-debian12-Linux-x86_64.deb.asc keel-core-0.5.5-debian12-Linux-x86_64.deb

# Verify SHA256SUMS
gpg --verify SHA256SUMS.asc
sha256sum -c SHA256SUMS
```

## Troubleshooting

### "PACKAGE_SIGNING_PRIVATE_KEY secret not configured"

Behaviour depends on the trigger:

- **Tagged releases (`refs/tags/*`)** — the `package-linux` workflow
  hard-fails when this secret is missing. Tagged releases must publish
  signed artifacts; an unsigned tagged release is not allowed. Either
  configure the secret as described in Step 3, or delete the tag.
- **Non-tag builds (branches, PRs)** — a warning is logged and the
  build produces unsigned artifacts, intended for development and
  pre-release validation only. These artifacts must not be promoted to
  a release.

This matches the signing-required policy in
[PRODUCTION_READINESS.md](PRODUCTION_READINESS.md) for release artifacts.

### "error receiving key from agent: Frase-senha incorreta"

The passphrase in `PACKAGE_SIGNING_PASSPHRASE` secret doesn't match the key's passphrase.

**Solution:** Regenerate the key or update the secret with the correct passphrase.

### "gpg: key not trusted"

When users verify signatures, they may see this message if they haven't explicitly trusted the key.

**User solution:**
```bash
gpg --edit-key <KEY_ID>
# At gpg> prompt, type: trust
# Select 5 (ultimate trust)
# Confirm with: y
# Exit with: quit
```

## Security Best Practices

1. **Keep the private key secure:** Never commit `signing-key.asc` to version control
2. **Use a strong passphrase:** At least 16 characters, mix of upper/lower/numbers/symbols
3. **Rotate keys periodically:** Consider expiring keys and generating new ones annually
4. **Store backups securely:** Keep a backup of `signing-key.asc` in a secure location (encrypted, offline)
5. **Publish public key:** Make your public key available for users to import:
   ```bash
   gpg --armor --export <KEY_ID> > KEEL-SIGNING-KEY.pub
   git add KEEL-SIGNING-KEY.pub && git commit -m "docs: add KEEL release signing public key"
   ```

## References

- [GPG Manual](https://www.gnupg.org/documentation/)
- [GitHub Actions Secrets Documentation](https://docs.github.com/en/actions/security-guides/encrypted-secrets)
- [CycloneDX SBOM Format](https://cyclonedx.org/)
