import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from app.core.config import settings


def send_password_reset_email(to_email: str, reset_link: str):
    """
    发送密码重置邮件
    """
    sender = settings.sender_email
    password = settings.sender_password

    if not sender or not password:
        print("❌ 未配置 SENDER_EMAIL 或 SENDER_PASSWORD，跳过发送密码重置邮件")
        return

    msg = MIMEMultipart()
    msg["Subject"] = "🔐 重置您的 SHL Solver 密码"
    msg["From"] = sender
    msg["To"] = to_email

    html_content = f"""
    <html>
      <body>
        <h2>密码重置请求</h2>
        <p>我们收到了重置您 SHL Solver 账户密码的请求。</p>
        <p>请点击下面的链接重置您的密码：</p>
        <p><a href="{reset_link}">重置密码</a></p>
        <p>如果您没有请求重置密码，请忽略此邮件。</p>
        <p>链接有效期为 15 分钟。</p>
      </body>
    </html>
    """

    msg.attach(MIMEText(html_content, "html", "utf-8"))

    try:
        server = smtplib.SMTP_SSL(settings.smtp_server, settings.smtp_port)
        server.login(sender, password)
        server.sendmail(sender, [to_email], msg.as_string())
        server.quit()
        print(f"✅ 密码重置邮件已发送至: {to_email}")
    except Exception as e:
        print(f"❌ 发送密码重置邮件失败: {e}")
