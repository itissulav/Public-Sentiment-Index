# app/utils/mailer.py
# Sends transactional emails via Resend (resend.com — free tier, no SMTP needed).
# Set RESEND_API_KEY in .env  |  pip install resend

import os


def send_analysis_ready_email(to_email: str, first_name: str, topic_name: str, app_base_url: str):
    api_key = os.getenv("RESEND_API_KEY", "")
    if not api_key:
        print("[mailer] RESEND_API_KEY not set — skipping email notification")
        return

    try:
        import resend
        resend.api_key = api_key
    except ImportError:
        print("[mailer] 'resend' package not installed — run: pip install resend")
        return

    history_url = f"{app_base_url.rstrip('/')}/history"

    html = f"""<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0"></head>
<body style="margin:0;padding:0;background:#0b0f19;font-family:'Inter',Arial,sans-serif;">
  <table width="100%" cellpadding="0" cellspacing="0" style="background:#0b0f19;padding:40px 0;">
    <tr>
      <td align="center">
        <table width="600" cellpadding="0" cellspacing="0"
               style="background:rgba(22,32,52,0.98);border:1px solid rgba(255,255,255,0.08);
                      border-radius:20px;overflow:hidden;max-width:600px;">

          <tr>
            <td style="background:linear-gradient(135deg,#4facfe,#00f2fe);padding:4px 0;"></td>
          </tr>

          <tr>
            <td style="padding:40px 44px 36px;">

              <p style="margin:0 0 28px;font-size:0.85rem;font-weight:700;letter-spacing:2px;
                         text-transform:uppercase;color:#4facfe;">
                Public Sentiment Index
              </p>

              <h1 style="margin:0 0 12px;font-size:1.6rem;font-weight:800;color:#f8fafc;line-height:1.3;">
                Your analysis is ready, {first_name} &#127881;
              </h1>

              <p style="margin:0 0 24px;font-size:0.95rem;color:#94a3b8;line-height:1.65;">
                The Reddit sentiment analysis for
                <strong style="color:#f8fafc;">"{topic_name}"</strong>
                has finished. Your results — charts, keyword insights, and sentiment trends —
                are now saved to your account.
              </p>

              <table cellpadding="0" cellspacing="0" style="margin:0 0 32px;">
                <tr>
                  <td style="background:linear-gradient(135deg,#4facfe,#00f2fe);
                              border-radius:30px;padding:14px 36px;">
                    <a href="{history_url}"
                       style="color:#0b0f19;font-size:0.95rem;font-weight:700;text-decoration:none;">
                      View Your Analysis &rarr;
                    </a>
                  </td>
                </tr>
              </table>

              <p style="margin:0 0 8px;font-size:0.82rem;color:#64748b;line-height:1.6;">
                Open your <strong style="color:#94a3b8;">Scan History</strong> and click
                <strong style="color:#94a3b8;">View Analysis</strong> next to
                <em>"{topic_name}"</em>.
              </p>

              <p style="margin:28px 0 0;font-size:0.8rem;color:#475569;">
                You're receiving this because you requested a sentiment analysis on PSI.
                If you didn't, you can safely ignore this email.
              </p>

            </td>
          </tr>

          <tr>
            <td style="padding:18px 44px;border-top:1px solid rgba(255,255,255,0.05);">
              <p style="margin:0;font-size:0.75rem;color:#334155;">
                &copy; Public Sentiment Index &nbsp;&middot;&nbsp; Powered by Reddit &amp; HuggingFace AI
              </p>
            </td>
          </tr>

        </table>
      </td>
    </tr>
  </table>
</body>
</html>"""

    try:
        resend.Emails.send({
            "from":    "Public Sentiment Index <onboarding@resend.dev>",
            "to":      [to_email],
            "subject": f"Your analysis for '{topic_name}' is ready",
            "html":    html,
        })
        print(f"[mailer] Email sent to {to_email} for topic '{topic_name}'")
    except Exception as e:
        print(f"[mailer] Failed to send email to {to_email}: {e}")
