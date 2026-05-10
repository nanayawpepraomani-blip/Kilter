# Kilter — Getting Started (Windows)

This guide gets you from zero to logged in. Read it top to bottom before
doing anything — the whole process takes about 5 minutes.

---

## What you need

- Windows 10 or 11
- Python 3.13 or newer (free — see Step 1 if you don't have it)
- An authenticator app on your phone (free — see Step 3)

---

## Step 1 — Install Python (skip if already installed)

1. Go to **https://www.python.org/downloads/** and download Python 3.13
   or newer (the big yellow "Download Python 3.x.x" button).
2. Run the installer.  
   **Important:** tick **"Add Python to PATH"** before you click Install.
3. When it finishes, open a Command Prompt and type `python --version`.
   You should see something like `Python 3.13.x`.

---

## Step 2 — Install an authenticator app (skip if already have one)

Kilter uses one-time codes (TOTP) for login — the same technology banks
and Google use for two-factor authentication. You need an app on your
phone that generates these codes.

Any of these work:
- **Microsoft Authenticator** (iOS / Android) — recommended
- **Google Authenticator** (iOS / Android)
- **Authy** (iOS / Android / Desktop)

Install one on your phone before continuing.

---

## Step 3 — Start Kilter

1. Open the `Kilter` folder you were sent.
2. **Double-click `setup.bat`** — a black Command Prompt window will open.
3. The script installs everything and then starts the server.  
   The first time it runs it will print something like this:

```
==================================================================
  FIRST-RUN ADMIN ENROLLMENT

  http://localhost:8000/enroll?user=admin&token=AbCdEfGh...

  Step 1: Open the URL above in your browser.
  Step 2: Scan the QR code with any authenticator app.
  Step 3: Go to http://localhost:8000/login
          Username: admin   Code: 6-digit code from the app
==================================================================
```

4. The same link is also saved to a file called **`first_login.txt`**
   in the Kilter folder — open that file if you miss it in the window.

> **Keep the Command Prompt window open.** Closing it stops the server.

---

## Step 4 — Enroll your authenticator

1. Copy the `http://localhost:8000/enroll?user=admin&token=...` link from
   the Command Prompt (or from `first_login.txt`) and paste it into your
   browser.
2. A QR code will appear on the page.
3. Open your authenticator app on your phone:
   - **Microsoft Authenticator**: tap **+** → **Other account (Google, etc.)**
   - **Google Authenticator**: tap **+** → **Scan a QR code**
   - **Authy**: tap **+** → **Scan QR Code**
4. Point your camera at the QR code on the screen.
5. The app will show a 6-digit code that refreshes every 30 seconds.
6. Type that code into the **"Verification code"** field on the web page
   and click **Confirm**.
7. You will see a list of **recovery codes** — save these somewhere safe
   (screenshot or note). They let you regain access if you lose your phone.

---

## Step 5 — Log in

1. Go to **http://localhost:8000/login** in your browser.
2. Enter:
   - **Username:** `admin`
   - **Code:** the current 6-digit code from your authenticator app
3. Click **Log in**.

You're in. The dashboard will walk you through the rest.

---

## Stopping and restarting

- **Stop:** press `Ctrl+C` in the Command Prompt, or just close the window.
- **Start again:** double-click `setup.bat` again. You don't need to
  re-enroll — your login is saved.

---

## Troubleshooting

| Problem | Fix |
|---|---|
| `python` not recognized | Re-install Python and tick "Add Python to PATH" |
| `pip install` fails | Check your internet connection, then retry |
| "Invalid code" at login | Your phone clock may be off — enable automatic time in phone settings |
| Enrollment link expired | Delete `kilter.db` and run `setup.bat` again to get a fresh link |
| Port 8000 already in use | Close other apps, or edit `setup.bat` and change `--port 8000` to `--port 8080`, then use `http://localhost:8080` |
| `first_login.txt` not found | The enrollment URL is also printed in the Command Prompt window — scroll up |

---

## Questions?

Contact **Timeless Nypo Tech**: timelessnypotech@outlook.com  
Website: https://www.kilter-app.com
