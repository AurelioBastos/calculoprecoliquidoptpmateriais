"""
SEFAZ - Validacao de NF-e / NFC-e / CT-e a partir do proprio XML
Le o protocolo de autorizacao (infProt) e todos os eventos posteriores
(procEventoNFe / retEvento) e retorna o status do ULTIMO evento,
garantindo que cancelamento posterior a autorizacao seja detectado.
"""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
import xml.etree.ElementTree as ET
import re

router = APIRouter()


class SefazConsultaRequest(BaseModel):
    chave: str
    xml_content: str = ""


class SefazConsultaResponse(BaseModel):
    cStat:    str
    xMotivo:  str
    tpAmb:    str
    modelo:   str
    nProt:    str = ""
    dhRecbto: str = ""
    fonte:    str = "xml"


_NS = re.compile(r'\{[^}]+\}')

def _strip(tag):
    return _NS.sub('', tag)

def _text(el, tag):
    for child in el.iter():
        if _strip(child.tag) == tag:
            return (child.text or '').strip()
    return ''

def _chave_info(chave44):
    if len(chave44) != 44:
        return {}
    return {'cUF': chave44[:2], 'tpAmb': chave44[20], 'modelo': chave44[20:22]}

_TIPO_EVENTO = {
    '110111': 'Cancelamento pelo emitente',
    '110112': 'Cancelamento por substituicao',
    '110110': 'Carta de Correcao',
    '110140': 'EPEC',
    '110150': 'Manifestacao do Destinatario',
}

_STATUS_MAP = {
    '100': ('autorizada', 'Autorizada'),
    '101': ('cancelada',  'Cancelada'),
    '110': ('denegada',   'Denegada'),
    '135': ('cancelada',  'Cancelada'),
    '150': ('autorizada', 'Autorizada (fora de prazo)'),
    '151': ('cancelada',  'Cancelada (fora de prazo)'),
    '155': ('cancelada',  'Cancelada (fora de prazo)'),
    '301': ('denegada',   'Denegada - irreg. emitente'),
    '302': ('denegada',   'Denegada - irreg. destinatario'),
}

def _status(cStat):
    return _STATUS_MAP.get(cStat, ('erro', f'cStat {cStat}'))


def _coletar_eventos(root):
    """
    Coleta todos os eventos do XML em ordem cronologica.
    Prioridade: infProt (autorizacao) + todos os procEventoNFe (cancelamento, CCe, etc.)
    Retorna lista de dicts ordenada por dhRegEvento.
    """
    eventos = []

    # 1. Protocolo de autorizacao principal (infProt)
    for el in root.iter():
        if _strip(el.tag) == 'infProt':
            cStat    = _text(el, 'cStat')
            xMotivo  = _text(el, 'xMotivo')
            nProt    = _text(el, 'nProt')
            dhRecbto = _text(el, 'dhRecbto')
            if cStat:
                eventos.append({
                    'cStat':       cStat,
                    'xMotivo':     xMotivo,
                    'nProt':       nProt,
                    'dhRegEvento': dhRecbto,
                    'descEvento':  'Autorizacao de Uso',
                })
            break

    # 2. Eventos (procEventoNFe contem o evento + retorno SEFAZ juntos)
    for proc_ev in root.iter():
        if _strip(proc_ev.tag) != 'procEventoNFe':
            continue

        tpEvento   = _text(proc_ev, 'tpEvento')
        dhEvento   = _text(proc_ev, 'dhEvento')
        xDescEv    = _text(proc_ev, 'xDescEv') or _TIPO_EVENTO.get(tpEvento, f'Evento {tpEvento}')
        dhRegEvento= _text(proc_ev, 'dhRegEvento') or dhEvento

        # cStat do retorno SEFAZ (resposta ao evento)
        # Fica dentro de retEvento > infEvento
        cStat_ev   = ''
        xMotivo_ev = ''
        nProt_ev   = ''

        for ret_ev in proc_ev.iter():
            if _strip(ret_ev.tag) == 'infEvento':
                # Pega o infEvento que tem cStat (o de retorno, nao o de envio)
                c = _text(ret_ev, 'cStat')
                if c and c != _text(proc_ev, 'cStat'):
                    # Ha dois infEvento: envio e retorno. O retorno tem cStat 135/155
                    pass
                # Busca o cStat mais especifico de retorno
                if not cStat_ev:
                    cStat_ev   = c
                    xMotivo_ev = _text(ret_ev, 'xMotivo')
                    nProt_ev   = _text(ret_ev, 'nProt')

        # Fallback: pega qualquer cStat dentro do bloco
        if not cStat_ev:
            for el in proc_ev.iter():
                t = _strip(el.tag)
                if t == 'cStat'      and not cStat_ev:   cStat_ev   = (el.text or '').strip()
                if t == 'xMotivo'    and not xMotivo_ev: xMotivo_ev = (el.text or '').strip()
                if t == 'nProt'      and not nProt_ev:   nProt_ev   = (el.text or '').strip()

        if cStat_ev:
            eventos.append({
                'cStat':       cStat_ev,
                'xMotivo':     xMotivo_ev or xDescEv,
                'nProt':       nProt_ev,
                'dhRegEvento': dhRegEvento,
                'descEvento':  xDescEv,
            })

    # Ordena cronologicamente (formato ISO e ordenavel como string)
    eventos.sort(key=lambda e: e['dhRegEvento'])
    return eventos


def _validar_xml(xml_str, chave44):
    info   = _chave_info(chave44)
    tpAmb  = info.get('tpAmb', '1')
    modelo = info.get('modelo', '55')

    try:
        root = ET.fromstring(xml_str.encode('utf-8') if isinstance(xml_str, str) else xml_str)
    except ET.ParseError:
        return SefazConsultaResponse(
            cStat='999', xMotivo='XML invalido ou corrompido',
            tpAmb=tpAmb, modelo=modelo,
        )

    for el in root.iter():
        if _strip(el.tag) == 'tpAmb':
            tpAmb = (el.text or '').strip() or tpAmb
            break

    eventos = _coletar_eventos(root)

    if not eventos:
        return SefazConsultaResponse(
            cStat='999',
            xMotivo='Protocolo SEFAZ nao encontrado. Use o XML com protocolo (nfeProc).',
            tpAmb=tpAmb, modelo=modelo,
        )

    # Ultimo evento = status atual
    ultimo = eventos[-1]

    # Se houver multiplos eventos, mostra o historico resumido
    if len(eventos) > 1:
        historico = ' → '.join(e['descEvento'] for e in eventos)
        xMotivo_final = f"{ultimo['xMotivo']} [{historico}]"
    else:
        xMotivo_final = ultimo['xMotivo']

    if ultimo['nProt']:
        xMotivo_final += f" · Prot: {ultimo['nProt']}"
    if ultimo['dhRegEvento']:
        xMotivo_final += f" · {ultimo['dhRegEvento'][:10]}"

    return SefazConsultaResponse(
        cStat    = ultimo['cStat'],
        xMotivo  = xMotivo_final,
        tpAmb    = tpAmb,
        modelo   = modelo,
        nProt    = ultimo['nProt'],
        dhRecbto = ultimo['dhRegEvento'],
        fonte    = 'xml',
    )


@router.post("/api/sefaz-consulta", response_model=SefazConsultaResponse)
async def sefaz_consulta(req: SefazConsultaRequest):
    chave = req.chave.strip().replace(' ', '')
    if len(chave) != 44 or not chave.isdigit():
        raise HTTPException(422, "Chave deve ter 44 digitos numericos.")
    if req.xml_content:
        return _validar_xml(req.xml_content, chave)
    raise HTTPException(400, "Envie o conteudo do XML no campo xml_content.")
