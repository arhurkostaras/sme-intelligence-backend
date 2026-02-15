const { Resend } = require('resend');

// Initialize Resend - graceful fallback when API key missing
const resend = process.env.RESEND_API_KEY ? new Resend(process.env.RESEND_API_KEY) : null;

const FROM_EMAIL = process.env.FROM_EMAIL || 'noreply@canadaaccountants.app';
const ADMIN_EMAIL = process.env.ADMIN_EMAIL || 'arthur@negotiateandwin.com';

/**
 * Shared email sender with graceful fallback
 */
async function sendEmail({ to, subject, html }) {
  if (!resend) {
    console.log(`[Email] RESEND_API_KEY not set. Would send to ${to}: "${subject}"`);
    return { success: false, reason: 'api_key_missing' };
  }

  try {
    const { data, error } = await resend.emails.send({
      from: FROM_EMAIL,
      to: Array.isArray(to) ? to : [to],
      subject,
      html,
    });

    if (error) {
      console.error('[Email] Resend API error:', error);
      return { success: false, reason: 'api_error', error };
    }

    console.log(`[Email] Sent to ${to}: "${subject}" (id: ${data.id})`);
    return { success: true, id: data.id };
  } catch (err) {
    console.error('[Email] Send failed:', err.message);
    return { success: false, reason: 'exception', error: err.message };
  }
}

/**
 * Subscription confirmation after Stripe checkout.session.completed
 */
async function sendSubscriptionConfirmation({ email, tier, interval }) {
  if (!email) return;

  const tierNames = {
    associate: 'Associate',
    professional: 'Professional',
    enterprise: 'Enterprise',
  };

  await sendEmail({
    to: email,
    subject: `Subscription Confirmed — ${tierNames[tier] || tier} Plan | CanadaAccountants`,
    html: `
      <h2>Subscription Confirmed!</h2>
      <p>Thank you for subscribing to the <strong>${tierNames[tier] || tier}</strong> plan (${interval || 'monthly'}).</p>
      <p>Your CPA profile is now active and will receive priority matching based on your tier.</p>
      <p><strong>Next steps:</strong></p>
      <ol>
        <li>Complete your CPA profile if you haven't already</li>
        <li>Client matches will begin appearing in your dashboard</li>
        <li>Manage your subscription anytime from your dashboard</li>
      </ol>
      <p><a href="https://canadaaccountants.app/cpa-dashboard.html">Go to Dashboard</a></p>
      <br>
      <p>Best regards,<br>Arthur Kostaras, CPA, CF<br>CanadaAccountants</p>
    `,
  });
}

/**
 * Payment receipt after invoice.payment_succeeded
 */
async function sendPaymentReceipt({ email, amount, currency, invoiceId }) {
  if (!email) return;

  const formattedAmount = (amount / 100).toFixed(2);
  const currencyLabel = (currency || 'cad').toUpperCase();

  await sendEmail({
    to: email,
    subject: `Payment Receipt — $${formattedAmount} ${currencyLabel} | CanadaAccountants`,
    html: `
      <h2>Payment Received</h2>
      <p>We've received your payment of <strong>$${formattedAmount} ${currencyLabel}</strong>.</p>
      <p><strong>Invoice ID:</strong> ${invoiceId || 'N/A'}</p>
      <p>Your subscription remains active. You can manage billing from your dashboard.</p>
      <p><a href="https://canadaaccountants.app/cpa-dashboard.html">Go to Dashboard</a></p>
      <br>
      <p>Best regards,<br>CanadaAccountants Billing</p>
    `,
  });
}

/**
 * Alert on invoice.payment_failed
 */
async function sendPaymentFailedAlert({ email, amount, currency, invoiceId }) {
  if (!email) return;

  const formattedAmount = (amount / 100).toFixed(2);
  const currencyLabel = (currency || 'cad').toUpperCase();

  await sendEmail({
    to: email,
    subject: 'Payment Failed — Action Required | CanadaAccountants',
    html: `
      <h2>Payment Failed</h2>
      <p>We were unable to process your payment of <strong>$${formattedAmount} ${currencyLabel}</strong>.</p>
      <p><strong>Invoice ID:</strong> ${invoiceId || 'N/A'}</p>
      <p>Please update your payment method to keep your subscription active:</p>
      <p><a href="https://canadaaccountants.app/cpa-dashboard.html">Update Payment Method</a></p>
      <p>If you need help, contact us at <strong>1.647.956.7290</strong> or reply to this email.</p>
      <br>
      <p>Best regards,<br>CanadaAccountants Billing</p>
    `,
  });

  // Alert admin
  await sendEmail({
    to: ADMIN_EMAIL,
    subject: `Payment Failed: ${email} — $${formattedAmount}`,
    html: `
      <h2>Payment Failure Alert</h2>
      <p><strong>Email:</strong> ${email}</p>
      <p><strong>Amount:</strong> $${formattedAmount} ${currencyLabel}</p>
      <p><strong>Invoice:</strong> ${invoiceId || 'N/A'}</p>
    `,
  });
}

module.exports = {
  sendEmail,
  sendSubscriptionConfirmation,
  sendPaymentReceipt,
  sendPaymentFailedAlert,
};
