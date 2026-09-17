import pytest
from unittest.mock import MagicMock
from decision_ledger.adapters.openai_adapter import AutoLedgerOpenAI
from decision_ledger.adapters.anthropic_adapter import AutoLedgerAnthropic
from decision_ledger.gatekeeper import Gatekeeper, GateAction

class MockOpenAICompletions:
    def create(self, model, messages, **kwargs):
        mock_resp = MagicMock()
        mock_resp.model = model
        mock_resp.choices = [
            MagicMock(message=MagicMock(content=f"Response from {model}"))
        ]
        return mock_resp

class MockOpenAIClient:
    def __init__(self):
        self.chat = MagicMock()
        self.chat.completions = MockOpenAICompletions()

class MockAnthropicMessages:
    def create(self, model, messages, max_tokens, **kwargs):
        mock_resp = MagicMock()
        mock_resp.model = model
        mock_content_block = MagicMock()
        mock_content_block.text = f"Anthropic response from {model}"
        mock_resp.content = [mock_content_block]
        return mock_resp

class MockAnthropicClient:
    def __init__(self):
        self.messages = MockAnthropicMessages()

def test_openai_adapter_delegation():
    base_client = MockOpenAIClient()
    gatekeeper = Gatekeeper()
    
    adapter = AutoLedgerOpenAI(
        openai_client=base_client,
        gatekeeper=gatekeeper,
        small_model="gpt-4o-mini",
        frontier_model="gpt-4o"
    )
    
    # Force DELEGATE -> small model
    gatekeeper.evaluate = MagicMock(return_value=GateAction.DELEGATE)
    
    resp = adapter.chat.completions.create(
        messages=[{"role": "user", "content": "Hello world"}]
    )
    assert resp.choices[0].message.content == "Response from gpt-4o-mini"
    assert len(adapter.shadow_logs) == 1

def test_openai_adapter_escalation():
    base_client = MockOpenAIClient()
    gatekeeper = Gatekeeper()
    
    adapter = AutoLedgerOpenAI(
        openai_client=base_client,
        gatekeeper=gatekeeper,
        small_model="gpt-4o-mini",
        frontier_model="gpt-4o"
    )
    
    # Force ESCALATE -> frontier model
    gatekeeper.evaluate = MagicMock(return_value=GateAction.ESCALATE)
    
    resp = adapter.chat.completions.create(
        messages=[{"role": "user", "content": "Complex coding problem"}]
    )
    assert resp.choices[0].message.content == "Response from gpt-4o"
    assert len(adapter.shadow_logs) == 1

def test_anthropic_adapter_delegation():
    base_client = MockAnthropicClient()
    gatekeeper = Gatekeeper()
    
    adapter = AutoLedgerAnthropic(
        anthropic_client=base_client,
        gatekeeper=gatekeeper,
        small_model="claude-3-haiku-20240307",
        frontier_model="claude-3-5-sonnet-20240620"
    )
    
    # Force DELEGATE -> small model
    gatekeeper.evaluate = MagicMock(return_value=GateAction.DELEGATE)
    
    resp = adapter.messages.create(
        messages=[{"role": "user", "content": "Hello Anthropic"}],
        max_tokens=100
    )
    assert resp.content[0].text == "Anthropic response from claude-3-haiku-20240307"
    assert len(adapter.shadow_logs) == 1
