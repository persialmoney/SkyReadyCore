"""
SkyReady transactional email templates (bundled with user-update Lambda).

Canonical copy for editing: ../shared/email_templates.py — keep both files in sync.

Each function returns a dict shaped for the SES send_email(Message=...) parameter,
containing a Subject and both Text and HTML Body variants.

Templates defined here:
- email_change_notification  — security notice sent to old address when email change is requested
"""

from typing import Any, Dict

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_BRAND = "SkyReady"
_SUPPORT = "support@skyready.app"


def _charset(data: str) -> Dict[str, str]:
    return {"Data": data, "Charset": "UTF-8"}


def _message(subject: str, text: str, html: str) -> Dict[str, Any]:
    """Assemble an SES Message dict."""
    return {
        "Subject": _charset(subject),
        "Body": {
            "Text": _charset(text),
            "Html": _charset(html),
        },
    }


# ---------------------------------------------------------------------------
# Templates
# ---------------------------------------------------------------------------

def email_change_notification(old_email: str, new_email: str, reset_url: str) -> Dict[str, Any]:
    """
    Security notice sent to the OLD email address when a sign-in email change
    is requested through the SkyReady app.

    The old address stays active for sign-in until the user verifies the new
    one (Cognito keep_original is enabled). The reset link lets the account
    owner regain control immediately if the request was not made by them.

    Args:
        old_email:  The current (old) sign-in address — used only for display.
        new_email:  The new address the change was requested to.
        reset_url:  Password-reset URL / deep link to include in the email.

    Returns:
        SES Message dict suitable for ses.send_email(Message=...).
    """
    subject = f"{_BRAND}: your sign-in email change request"

    text = (
        f"Hi,\n\n"
        f"A request was made to change the sign-in email address on your {_BRAND} account "
        f"({old_email}) to {new_email}.\n\n"
        f"A verification code has been sent to {new_email}. Your current email address "
        f"({old_email}) will remain active for sign-in until that code is confirmed.\n\n"
        f"If you did not make this request, your account may be at risk. "
        f"Reset your password immediately using the link below:\n\n"
        f"{reset_url}\n\n"
        f"If you did request this change, you can ignore this message.\n\n"
        f"— The {_BRAND} team\n"
        f"Questions? Contact us at {_SUPPORT}"
    )

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>{subject}</title>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Helvetica, Arial, sans-serif;
           background: #f3f4f6; margin: 0; padding: 0; color: #111827; }}
    .wrapper {{ max-width: 560px; margin: 40px auto; background: #ffffff;
                border-radius: 12px; overflow: hidden;
                box-shadow: 0 1px 3px rgba(0,0,0,0.1); }}
    .header {{ background: #1d4ed8; padding: 28px 32px; }}
    .header h1 {{ color: #ffffff; font-size: 20px; margin: 0; font-weight: 600; }}
    .body {{ padding: 32px; }}
    .body p {{ font-size: 15px; line-height: 1.6; margin: 0 0 16px; color: #374151; }}
    .highlight {{ background: #eff6ff; border-left: 4px solid #1d4ed8;
                  border-radius: 4px; padding: 12px 16px; margin: 20px 0; }}
    .highlight p {{ margin: 0; font-size: 14px; color: #1e40af; }}
    .button {{ display: inline-block; margin: 8px 0 24px;
               background: #dc2626; color: #ffffff; text-decoration: none;
               padding: 12px 24px; border-radius: 8px; font-size: 15px; font-weight: 600; }}
    .footer {{ padding: 20px 32px; background: #f9fafb;
               border-top: 1px solid #e5e7eb; font-size: 13px; color: #6b7280; }}
    .footer a {{ color: #1d4ed8; text-decoration: none; }}
  </style>
</head>
<body>
  <div class="wrapper">
    <div class="header">
      <h1>{_BRAND} Security Notice</h1>
    </div>
    <div class="body">
      <p>Hi,</p>
      <p>
        A request was made to change the sign-in email address on your {_BRAND} account
        (<strong>{old_email}</strong>) to <strong>{new_email}</strong>.
      </p>
      <div class="highlight">
        <p>
          A verification code has been sent to <strong>{new_email}</strong>.
          Your current email (<strong>{old_email}</strong>) will remain active
          for sign-in until the new address is confirmed.
        </p>
      </div>
      <p>
        <strong>Didn't make this request?</strong> Your account may be at risk.
        Reset your password immediately:
      </p>
      <a class="button" href="{reset_url}">Reset my password</a>
      <p>If you did request this change, you can safely ignore this email.</p>
    </div>
    <div class="footer">
      <p>
        Questions? Contact us at <a href="mailto:{_SUPPORT}">{_SUPPORT}</a>
      </p>
      <p>
        You're receiving this because a change was requested on your {_BRAND} account.
      </p>
    </div>
  </div>
</body>
</html>"""

    return _message(subject, text, html)
