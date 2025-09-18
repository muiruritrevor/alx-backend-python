from rest_framework import serializers
from .models import User, Conversation, Message
# from django.contrib.auth.models import AbstractUser

class UserSerializer(serializers.ModelSerializer):
    class Meta:
        model = User
        fields = ['id', 'first_name', 'last_name', 'email', 'role']

class ConversationSerializer(serializers.ModelSerializer):
    participants = UserSerializer(many=True, read_only=True)  # Nested for output
    participant_ids = serializers.ListField(
        child=serializers.UUIDField(), write_only=True
    )  # For creating/updating

    class Meta:
        model = Conversation
        fields = ['id', 'participants', 'participant_ids', 'created_at']
        read_only_fields = ['id', 'created_at']

    def create(self, validated_data):
        participant_ids = validated_data.pop('participant_ids')
        participants = User.objects.filter(id__in=participant_ids)
        if not participants.exists():
            raise serializers.ValidationError({"participant_ids": "Invalid user IDs"})
        if len(participants) < 2:
            raise serializers.ValidationError({"participant_ids": "At least 2 participants required"})
        conversation = Conversation.objects.create()
        conversation.participants.set(participants)
        return conversation

class MessageSerializer(serializers.ModelSerializer):
    sender = UserSerializer(read_only=True)
    conversation = serializers.PrimaryKeyRelatedField(queryset=Conversation.objects.all())

    class Meta:
        model = Message
        fields = ['id', 'sender', 'conversation', 'message_body', 'timestamp']
        read_only_fields = ['id', 'sender', 'timestamp']

    def create(self, validated_data):
        # Set sender as the authenticated user
        validated_data['sender'] = self.context['request'].user
        return super().create(validated_data)

    def validate(self, attrs):
        # Ensure sender is in the conversation
        conversation = attrs['conversation']
        if not conversation.participants.filter(id=self.context['request'].user.id).exists():
            raise serializers.ValidationError({"conversation": "You are not a participant"})
        return attrs