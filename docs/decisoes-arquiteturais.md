# Decisões Arquiteturais

Registro das decisões, riscos mitigados e estratégias de resiliência da Saga de compra. O contexto e os diagramas estão no [README](../README.md).

## 1. Resumo das decisões

| ID | Decisão | Motivo principal |
|---|---|---|
| ADR-01 | Saga baseada em **Orquestração** | Fluxo ordenado, compensações encadeadas, boa observabilidade |
| ADR-02 | **Transactional Outbox** para publicar mensagens | Atualizar o banco e publicar a mensagem de forma atômica |
| ADR-03 | **Idempotent Consumer** em todos os serviços | Barramento entrega *at-least-once* |
| ADR-04 | **Retry com backoff exponencial e jitter** | Falhas transitórias de rede |
| ADR-05 | **Circuit Breaker** na chamada ao gateway | Evitar cascata de falhas e esgotamento de recursos |
| ADR-06 | **Dead Letter Queue (DLQ)** | Isolar mensagens que não conseguem ser processadas |
| ADR-07 | Resposta `202 Accepted` + polling | O fluxo é assíncrono, o cliente consulta o resultado |
| ADR-08 | *Semantic lock* (pedido `PENDING`) | Compensar a falta de isolamento (o "I" do ACID) |

## 2. Detalhamento

### ADR-01: Orquestração em vez de Coreografia

Ver a justificativa e a matriz de decisão no README. O orquestrador fica **sem regra de negócio** (apenas coordena), tem o estado persistido em `saga_state` e roda em múltiplas instâncias. A concorrência entre instâncias é controlada por *optimistic locking* (`version`), de modo que duas instâncias nunca avançam a mesma saga ao mesmo tempo.

### ADR-02: Transactional Outbox

Um serviço precisa gravar no banco **e** publicar a mensagem. Sem cuidado, o banco confirma e a publicação falha (ou o inverso), travando a saga. Cada serviço grava a mensagem em uma tabela `outbox` **na mesma transação local** do efeito de negócio, e um processo separado (*relay* ou CDC) publica as mensagens pendentes no broker. Se o relay cair, a mensagem continua gravada e será publicada depois (garantia *at-least-once*, por isso o ADR-03).

### ADR-03: Idempotent Consumer

Toda mensagem tem `messageId`. O consumidor registra o `messageId` em `processed_messages` na mesma transação do efeito de negócio, e duplicatas apenas reenviam a resposta guardada. Detalhes no README, seção 4.1.

### ADR-04: Retry

- **Onde:** chamadas síncronas ao gateway e reentregas de mensagens com falha transitória.
- **Como:** backoff exponencial com *jitter* (ex.: 1s, 2s, 4s, 8s com variação aleatória) e limite de tentativas, para não sincronizar os clientes e sobrecarregar o serviço que está se recuperando.
- **O que não repetir:** falhas de negócio (cartão recusado, saldo insuficiente). Essas **não** são erros técnicos: geram a resposta `PaymentRejected` e iniciam a compensação.

### ADR-05: Circuit Breaker

O Payment Service envolve a chamada ao gateway em um *circuit breaker*:

- **Fechado:** as chamadas passam normalmente.
- **Aberto:** quando a taxa de falhas ultrapassa um limite (ex.: 50% em uma janela de chamadas), novas chamadas falham imediatamente, sem esperar timeout.
- **Semiaberto:** após um intervalo, algumas chamadas de teste decidem se o circuito volta a fechar.

Com o circuito aberto, o comando `ProcessPaymentCommand` **não é descartado**: o Payment responde com falha transitória, e a mensagem volta para retry (ou o `step_deadline` do orquestrador cuida do reenvio). Isso preserva a consistência sem derrubar o serviço.

### ADR-06: Dead Letter Queue

Uma mensagem vai para a DLQ quando excede o número máximo de tentativas ou quando é inválida (*poison message*, ex.: payload que não pode ser desserializado).

- Cada mensagem na DLQ mantém o `sagaId`, o motivo da falha e o número de tentativas.
- Há **alerta** quando a DLQ recebe mensagens, e a saga correspondente fica em `NEEDS_ATTENTION`.
- Após a correção da causa, as mensagens podem ser **reprocessadas** (*redrive*) com segurança, graças à idempotência.
- Mensagens na DLQ nunca são apagadas automaticamente sem análise.

### ADR-07: Resposta ao cliente

`POST /orders` devolve `202 Accepted` com o `orderId`. O cliente descobre o resultado por `GET /orders/{orderId}` (polling) ou por notificação (webhook ou websocket), evitando manter uma conexão HTTP aberta durante toda a saga.

### ADR-08: Falta de isolamento

Como cada passo confirma no seu banco antes de a saga terminar, outros processos podem enxergar estados intermediários. A contramedida adotada é o ***semantic lock***: o pedido existe como `PENDING`, e qualquer operação que dependa dele (edição, segunda cobrança) é rejeitada enquanto a saga estiver em andamento. A reserva de estoque também é um bloqueio semântico: o item sai do estoque disponível sem ser considerado vendido.

## 3. Riscos mitigados

| Risco | Impacto | Mitigação |
|---|---|---|
| Mensagem duplicada ou reentregue | Cobrança ou reserva em dobro | Idempotent Consumer, chave de idempotência no gateway |
| Banco confirma e a mensagem não é publicada | Saga travada no meio | Transactional Outbox |
| Timeout de rede na resposta do pagamento | Estado ambíguo (cobrou ou não?) | Consulta de status pela chave de idempotência antes de compensar |
| Resposta tardia após a compensação | Pedido cancelado com cobrança ativa | Comparação com `status` e `version` da saga, estorno automático |
| Gateway lento ou fora do ar | Esgotamento de threads, falha em cascata | Timeout curto, Retry com backoff, Circuit Breaker |
| Mensagem que nunca consegue ser processada | Fila bloqueada, saga presa | DLQ com alerta e *redrive* |
| Compensação que falha | Estoque preso e pedido inconsistente | A compensação é retentada até concluir (é idempotente) e, se esgotar, `NEEDS_ATTENTION` |
| Queda do orquestrador no meio da saga | Saga esquecida | Estado persistido em `saga_state` e *job* de recuperação por `step_deadline` |
| Leitura de estado intermediário | Pedido editado durante a saga | Semantic lock (`PENDING`) |
| Reserva de estoque que nunca é confirmada | Produto preso para sempre | Reserva com **expiração** (TTL), liberada automaticamente se a saga não avançar |

## 4. Resiliência: resumo por camada

| Camada | Estratégia |
|---|---|
| Mensageria | Outbox, entrega *at-least-once*, DLQ, `messageId` |
| Consumidores | Idempotent Consumer, transação local única |
| Chamada externa | Timeout, Retry com backoff e jitter, Circuit Breaker |
| Orquestração | Estado persistido, `step_deadline`, *optimistic locking*, recuperação periódica |
| Negócio | Compensações idempotentes, reserva com TTL, semantic lock |
| Operação | `sagaId` como *correlation id* em logs e traces, métricas de sagas por status, alertas de DLQ e `NEEDS_ATTENTION` |

## 5. Consistência eventual

Entre o início e o fim da saga, o sistema está em estado intermediário, e isso é aceito explicitamente: o pedido fica `PENDING` até chegar a um estado final (`APPROVED` ou `CANCELLED`). Ao final, **todos os serviços convergem** para um estado consistente, seja pela conclusão do fluxo de sucesso, seja pela execução das compensações.
