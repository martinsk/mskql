/*
 * tutorial-interactive.js — Embeds the mskql WASM engine into tutorial pages.
 * Turns static SQL code blocks into live, editable, runnable steps.
 */
(function() {
    'use strict';

    var db = new MskqlDB();
    var blocks = [];      /* { textarea, highlightEl, resultEl, statusEl, sql } */
    var barStatus;
    var allBtns = [];

    /* ── Helpers ──────────────────────────────────────────────── */

    function escapeHtml(s) {
        return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
    }

    function renderResult(res, resultEl, statusEl) {
        if (!res.ok) {
            statusEl.className = 'live-status error';
            statusEl.textContent = res.output;
            resultEl.innerHTML = '';
            return;
        }
        var output = res.output.trim();
        if (output === 'OK' || output === '') {
            statusEl.className = 'live-status';
            statusEl.textContent = 'OK';
            resultEl.innerHTML = '';
            return;
        }
        var lines = output.split('\n').filter(function(l) { return l.length > 0; });
        if (lines.length === 0) {
            statusEl.className = 'live-status';
            statusEl.textContent = '0 rows';
            resultEl.innerHTML = '';
            return;
        }
        var headers = lines[0].split('\t');
        var dataLines = lines.slice(1);
        statusEl.className = 'live-status';
        statusEl.textContent = dataLines.length + ' row' + (dataLines.length !== 1 ? 's' : '');

        var html = '<table class="result-table"><thead><tr>';
        for (var j = 0; j < headers.length; j++) {
            html += '<th>' + escapeHtml(headers[j]) + '</th>';
        }
        html += '</tr></thead><tbody>';
        for (var i = 0; i < dataLines.length; i++) {
            var cells = dataLines[i].split('\t');
            html += '<tr>';
            for (var k = 0; k < cells.length; k++) {
                if (cells[k] === 'NULL') {
                    html += '<td class="null-val">NULL</td>';
                } else if (/^-?\d+(\.\d+)?$/.test(cells[k])) {
                    html += '<td class="num-val">' + escapeHtml(cells[k]) + '</td>';
                } else {
                    html += '<td>' + escapeHtml(cells[k]) + '</td>';
                }
            }
            html += '</tr>';
        }
        html += '</tbody></table>';
        resultEl.innerHTML = html;
    }

    function syncHighlight(textarea, codeEl) {
        codeEl.textContent = textarea.value + '\n';
        delete codeEl.dataset.highlighted;
        hljs.highlightElement(codeEl);
    }

    function createEditor(originalPre) {
        var sql = originalPre.textContent;
        var wrap = document.createElement('div');
        wrap.className = 'tut-editor-wrap';

        var pre = document.createElement('pre');
        var code = document.createElement('code');
        code.className = 'language-sql';
        code.textContent = sql + '\n';
        pre.appendChild(code);

        var ta = document.createElement('textarea');
        ta.spellcheck = false;
        ta.value = sql;

        wrap.appendChild(pre);
        wrap.appendChild(ta);

        ta.addEventListener('input', function() {
            syncHighlight(ta, code);
        });
        ta.addEventListener('scroll', function() {
            pre.scrollTop = ta.scrollTop;
            pre.scrollLeft = ta.scrollLeft;
        });
        ta.addEventListener('keydown', function(e) {
            if (e.key === 'Tab') {
                e.preventDefault();
                var start = ta.selectionStart;
                var end = ta.selectionEnd;
                ta.value = ta.value.substring(0, start) + '    ' + ta.value.substring(end);
                ta.selectionStart = ta.selectionEnd = start + 4;
                syncHighlight(ta, code);
            }
        });

        /* initial highlight */
        hljs.highlightElement(code);

        return { wrap: wrap, textarea: ta, codeEl: code };
    }

    function runBlock(block) {
        var sql = block.textarea.value.trim();
        if (!sql) return;
        var res = db.exec(sql);
        if (block.resultEl) {
            renderResult(res, block.resultEl, block.statusEl);
        } else if (block.statusEl) {
            /* setup block — just show status */
            if (res.ok) {
                block.statusEl.className = 'setup-status ok';
                block.statusEl.textContent = '\u2713 OK';
            } else {
                block.statusEl.className = 'setup-status error';
                block.statusEl.textContent = res.output;
            }
        }
        return res.ok;
    }

    /* ── Build the interactive bar ────────────────────────────── */

    function buildBar() {
        var bar = document.createElement('div');
        bar.className = 'tutorial-bar';

        barStatus = document.createElement('span');
        barStatus.className = 'bar-status';
        barStatus.textContent = 'Loading\u2026';

        var resetBtn = document.createElement('button');
        resetBtn.textContent = 'Reset DB';
        resetBtn.disabled = true;
        resetBtn.addEventListener('click', function() {
            db.reset();
            barStatus.className = 'bar-status';
            barStatus.textContent = 'Database reset.';
            /* clear all results */
            blocks.forEach(function(b) {
                if (b.resultEl) b.resultEl.innerHTML = '';
                if (b.statusEl) {
                    b.statusEl.className = b.resultEl ? 'live-status' : 'setup-status';
                    b.statusEl.textContent = '';
                }
            });
        });

        var runAllBtn = document.createElement('button');
        runAllBtn.className = 'run-all-btn';
        runAllBtn.textContent = '\u25B6 Run All Steps';
        runAllBtn.disabled = true;
        runAllBtn.addEventListener('click', function() {
            barStatus.className = 'bar-status';
            barStatus.textContent = 'Running all steps\u2026';
            var i = 0;
            function next() {
                if (i >= blocks.length) {
                    barStatus.textContent = 'All steps complete.';
                    return;
                }
                var ok = runBlock(blocks[i]);
                i++;
                if (!ok) {
                    barStatus.className = 'bar-status error';
                    barStatus.textContent = 'Stopped at step ' + i + ' (error).';
                    return;
                }
                setTimeout(next, 10);
            }
            next();
        });

        bar.appendChild(runAllBtn);
        bar.appendChild(resetBtn);
        bar.appendChild(barStatus);

        allBtns.push(resetBtn);
        allBtns.push(runAllBtn);

        /* Insert bar before the first h2 or at top of body */
        var firstH2 = document.querySelector('h2');
        if (firstH2) {
            firstH2.parentNode.insertBefore(bar, firstH2);
        } else {
            document.body.insertBefore(bar, document.body.firstChild);
        }
    }

    /* ── Enhance query-pair blocks ────────────────────────────── */

    function enhanceQueryPairs() {
        var pairs = document.querySelectorAll('.query-pair');
        pairs.forEach(function(pair) {
            pair.classList.add('interactive');
            var origPre = pair.querySelector('pre');
            if (!origPre) return;

            var editor = createEditor(origPre);

            /* Run button */
            var btn = document.createElement('button');
            btn.className = 'sql-run-btn';
            btn.textContent = '\u25B6 Run';
            btn.disabled = true;
            allBtns.push(btn);

            /* Live result area */
            var liveStatus = document.createElement('div');
            liveStatus.className = 'live-status';
            var liveResult = document.createElement('div');
            liveResult.className = 'live-result';
            liveResult.appendChild(liveStatus);

            /* Wrap existing result table in <details> */
            var resultWrap = pair.querySelector('.result-wrap');
            if (resultWrap) {
                var details = document.createElement('details');
                details.className = 'expected-result';
                var summary = document.createElement('summary');
                summary.textContent = 'Expected result';
                details.appendChild(summary);
                /* Move result-wrap contents into details */
                while (resultWrap.firstChild) {
                    details.appendChild(resultWrap.firstChild);
                }
                resultWrap.parentNode.removeChild(resultWrap);
                pair.appendChild(details);
            }

            /* Insert elements */
            pair.insertBefore(btn, pair.firstChild);
            pair.insertBefore(editor.wrap, btn.nextSibling);
            pair.appendChild(liveResult);

            var block = {
                textarea: editor.textarea,
                codeEl: editor.codeEl,
                resultEl: liveResult,
                statusEl: liveStatus
            };
            blocks.push(block);

            btn.addEventListener('click', function() {
                runBlock(block);
            });

            /* Ctrl+Enter to run */
            editor.textarea.addEventListener('keydown', function(e) {
                if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
                    e.preventDefault();
                    runBlock(block);
                }
            });
        });
    }

    /* ── Enhance standalone setup blocks ──────────────────────── */

    function enhanceSetupBlocks() {
        var allPres = document.querySelectorAll('pre');
        allPres.forEach(function(pre) {
            /* Skip if inside a query-pair (already handled) */
            if (pre.closest('.query-pair')) return;
            var code = pre.querySelector('code.language-sql');
            if (!code) return;

            var editor = createEditor(pre);

            var btn = document.createElement('button');
            btn.className = 'sql-run-btn';
            btn.textContent = '\u25B6 Run setup';
            btn.disabled = true;
            allBtns.push(btn);

            var status = document.createElement('div');
            status.className = 'setup-status';

            /* Replace original pre with button + editor + status */
            pre.parentNode.insertBefore(btn, pre);
            pre.parentNode.insertBefore(editor.wrap, pre);
            pre.parentNode.insertBefore(status, pre);
            pre.style.display = 'none';

            var block = {
                textarea: editor.textarea,
                codeEl: editor.codeEl,
                resultEl: null,
                statusEl: status
            };
            blocks.push(block);

            btn.addEventListener('click', function() {
                runBlock(block);
            });

            editor.textarea.addEventListener('keydown', function(e) {
                if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
                    e.preventDefault();
                    runBlock(block);
                }
            });
        });
    }

    /* ── Init ─────────────────────────────────────────────────── */

    function init() {
        buildBar();
        enhanceSetupBlocks();
        enhanceQueryPairs();

        db.init('../wasm/mskql.wasm').then(function() {
            barStatus.className = 'bar-status';
            barStatus.textContent = 'Database ready. Run steps in order, or click Run All.';
            allBtns.forEach(function(b) { b.disabled = false; });
        }).catch(function(err) {
            barStatus.className = 'bar-status error';
            barStatus.textContent = 'Failed to load WASM: ' + err.message;
            console.error(err);
        });
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }
})();
