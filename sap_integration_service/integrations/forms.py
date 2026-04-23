from django import forms
from django.core.exceptions import ValidationError

from integrations.crypto_helpers import encrypt_secret
from integrations.models import SapClientCredential


class SapClientCredentialAdminForm(forms.ModelForm):
    sap_password = forms.CharField(
        label="Senha SAP (Service Layer)",
        widget=forms.PasswordInput(render_value=False),
        required=False,
        help_text="Obrigatória ao criar. Em edição, deixe em branco para manter a senha atual.",
    )

    class Meta:
        model = SapClientCredential
        fields = (
            "client_code",
            "client_name",
            "company_id",
            "base_url",
            "company_db",
            "username",
            "active",
        )

    def clean(self):
        cleaned = super().clean()
        pwd = (cleaned.get("sap_password") or "").strip()
        if not self.instance.pk and not pwd:
            raise ValidationError({"sap_password": "Informe a senha ao cadastrar uma nova credencial."})
        return cleaned

    def save(self, commit=True):
        instance = super().save(commit=False)
        pwd = (self.cleaned_data.get("sap_password") or "").strip()
        if pwd:
            try:
                instance.password_encrypted = encrypt_secret(pwd)
            except RuntimeError as exc:
                raise ValidationError({"sap_password": str(exc)}) from exc
        if commit:
            instance.save()
        return instance
