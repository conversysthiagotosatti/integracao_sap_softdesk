"""
Views da API REST: autenticação JWT obrigatória (Bearer) em todas as rotas /api/* deste módulo.
"""
from rest_framework.permissions import IsAuthenticated
from rest_framework.views import APIView

from security.authentication import BearerJWTAuthentication


class JWTAPIView(APIView):
    """Todas as rotas SAP exigem o mesmo token JWT do sistema principal."""

    authentication_classes = [BearerJWTAuthentication]
    permission_classes = [IsAuthenticated]
