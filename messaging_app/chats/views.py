from rest_framework import viewsets, status
# from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from django_filters.rest_framework import DjangoFilterBackend
from .models import Conversation, Message
from .serializers import ConversationSerializer, MessageSerializer

class ConversationViewSet(viewsets.ModelViewSet):
    serializer_class = ConversationSerializer
    # permission_classes = [IsAuthenticated]
    filter_backends = [DjangoFilterBackend]
    filterset_fields = ['created_at']

    def get_queryset(self):
        return Conversation.objects.filter(participants=self.request.user).prefetch_related('participants')

    def create(self, request, *args, **kwargs):
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        serializer.save()
        headers = self.get_success_headers(serializer.data)
        return Response(
            {"status": "success", "data": serializer.data},
            status=status.HTTP_201_CREATED,
            headers=headers
        )
    

class MessageViewSet(viewsets.ModelViewSet):
    serializer_class = MessageSerializer
    # permission_classes = [IsAuthenticated]
    filter_backends = [DjangoFilterBackend]
    filterset_fields = ['timestamp']  # No need for 'conversation' filter since it's in the URL

    def get_queryset(self):
        # Get conversation_id from URL (passed by NestedDefaultRouter)
        conversation_id = self.kwargs.get('conversation')
        # Filter messages by conversation and ensure user is a participant
        return Message.objects.filter(
            conversation_id=conversation_id,
            conversation__participants=self.request.user
        ).select_related('sender', 'conversation')

    def create(self, request, *args, **kwargs):
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        serializer.save()
        return Response(
            {"status": "success", "data": serializer.data},
            status=status.HTTP_201_CREATED
        )
