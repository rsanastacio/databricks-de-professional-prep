# Databricks Data Engineer Professional — kit de estudo

Kit de preparação final para o **Databricks Certified Data Engineer Professional**, versão do exame válida **a partir de 9-out-2026** (60 questões, 120 min, em inglês, 9 seções).

> As questões dos simulados são **originais, de prática**. Não são questões oficiais do exame. Os gabaritos seguem a documentação da Databricks, mas a fonte de verdade é o [guia oficial do exame](https://www.databricks.com/learn/certification/data-engineer-professional) e a [documentação](https://docs.databricks.com). Reconfirme as features antes da prova.

## Simulados (app)

**Abrir:** `https://<usuario>.github.io/<repo>/`

- 2 simulados × 60 questões, distribuídas pelos pesos oficiais dos domínios.
- **Feedback na hora**: mostra o veredito, explica o porquê e traz uma justificativa para **cada alternativa** (por que a correta está certa e por que as outras estão erradas).
- **Modo prova**: com o feedback desligado, a correção aparece só no fim, com score por domínio contra o corte de ~70%.
- Timer de 120 min, "revisar erradas", progresso salvo no aparelho.

### No iPhone
1. Abra a URL no **Safari**. Pela pré-visualização do app Arquivos/Mail não funciona, porque ela não roda JavaScript.
2. Compartilhar → **Adicionar à Tela de Início**.
3. Use sempre pelo ícone. Depois do primeiro acesso, ele **funciona offline**.

O progresso fica salvo no aparelho, separado por contexto: o ícone da Tela de Início e a aba do Safari não compartilham respostas.

Versão desktop (layout com todas as questões numa página): `desktop/`.

## Material

| Arquivo | Conteúdo |
|---|---|
| [docs/PLANO_ESTUDO.md](docs/PLANO_ESTUDO.md) | Plano de 14 dias por peso de domínio + cheat sheet + tracker |
| [docs/MATERIAL_ESTUDO.md](docs/MATERIAL_ESTUDO.md) | Recap dos 9 domínios, heurísticas de decisão, AUTO CDC/SCD |
| [docs/SIMULADO_1.md](docs/SIMULADO_1.md) · [docs/SIMULADO_2.md](docs/SIMULADO_2.md) | Questões + gabarito com explicações (fonte do app) |
| [docs/plano_cert_de_pro.ics](docs/plano_cert_de_pro.ics) | Agenda dos 14 dias para importar no calendário |

## Regenerar o app

Depois de editar `docs/SIMULADO_*.md` ou `data/just_*.json`:

```bash
python3 scripts/parse_simulados.py   # valida e gera desktop/questions.js
python3 scripts/build_mobile.py      # gera index.html, sw.js, manifest e ícones
```

O `sw.js` muda de versão a cada build, então quem já instalou recebe a atualização ao abrir o app com internet.
