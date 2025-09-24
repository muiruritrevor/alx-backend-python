from rest_framework import serializers
from rest_framework.exceptions import ValidationError
from .models import User, Conversation, Message


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
        participant_ids = validated_data.pop('participant_ids', [])
        participants = User.objects.filter(id__in=participant_ids)

        if not participants.exists():
            raise ValidationError({"participant_ids": "Invalid user IDs"})

        if len(participants) < 2:
            raise ValidationError({"participant_ids": "At least 2 participants required"})

        conversation = Conversation.objects.create()
        conversation.participants.set(participants)  # Links the users as participants.
        return conversation


class MessageSerializer(serializers.ModelSerializer):
    sender = UserSerializer(read_only=True)
    conversation = serializers.PrimaryKeyRelatedField(queryset=Conversation.objects.all())

    class Meta:
        model = Message
        fields = ['id', 'sender', 'conversation', 'message_body', 'sent_at']
        read_only_fields = ['id', 'sender', 'sent_at']

    def create(self, validated_data):
        user = self.context['request'].user

        if not user or not user.is_authenticated:
            raise ValidationError({"sender": "Authentication required to send a message."})

        validated_data['sender'] = user
        return super().create(validated_data)

    def validate(self, attrs):
        user = self.context['request'].user
        conversation = attrs['conversation']

        if not user.is_authenticated:
            raise ValidationError({"sender": "Authentication required."})

        if not conversation.participants.filter(id=user.id).exists():
            raise ValidationError({"conversation": "You are not a participant in this conversation."})

        return attrs