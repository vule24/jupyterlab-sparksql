import {
  JupyterFrontEnd,
  JupyterFrontEndPlugin
} from '@jupyterlab/application';
import { INotebookTracker } from '@jupyterlab/notebook';
import { CodeMirrorEditor } from '@jupyterlab/codemirror';
import { Cell, isCodeCellModel } from '@jupyterlab/cells';
import CodeMirror from 'codemirror';

interface IMagicSyntax {
  [key: string]: string;
}

const CellMagicSyntaxMap: IMagicSyntax = {
  ipython: 'text/x-ipython',
  '%%sql': 'text/x-pgsql',
  '%%sh': 'text/x-sh',
  '%%bash': 'text/x-sh',
  '%%html': 'text/html',
  '%%javascript': 'text/javascript',
  '%%js': 'text/javascript'
};

class SyntaxHighlighter {
  constructor(
    protected app: JupyterFrontEnd,
    protected tracker: INotebookTracker
  ) {
    // wait for JupyterLab page to startup/reload
    this.app.restored.then(() => {
      // On Notebook loaded
      this.tracker.currentWidget?.content.fullyRendered.connect(notebook => {
        // emit everytime cell rendered respectively
        if (this.tracker.currentWidget) {
          const length = this.tracker.currentWidget.content.widgets.length;
          this.setSyntax(
            this.tracker.currentWidget.content.widgets.slice(
              length - 1,
              length
            )[0]
          );
        }
      });
      // On current active cell content changed
      this.tracker.currentWidget?.content.modelContentChanged.connect(() => {
        this.tracker.activeCell && this.setSyntax(this.tracker.activeCell);
      });

      // On current notebook change
      this.tracker.currentChanged.connect(tracker => {
        tracker.restored.then(() => {
          // On notebook opened - cells added
          tracker.currentWidget?.model?.cells.changed.connect(
            (cellModels, change) => {
              // console.log(change.type, change.newIndex);
              if (change.type === 'add') {
                const widgets = tracker.currentWidget?.content.widgets;
                if (widgets) {
                  // console.log(widgets);
                  const cellWidget = widgets.find(widget => {
                    return (
                      widget.model.id === cellModels.get(change.newIndex).id
                    );
                  });
                  // console.log(cellWidget);
                  cellWidget && this.setSyntax(cellWidget);
                }
              }
            }
          );
          // On notebook model content changed - activeCell
          tracker.currentWidget?.model?.contentChanged.connect(() => {
            this.tracker.activeCell && this.setSyntax(this.tracker.activeCell);
          });
        });
      });
    });
  }

  private setSyntax(cell: Cell): void {
    const editor = (cell.editor as CodeMirrorEditor)
      .editor as CodeMirror.Editor;

    if (cell !== null && isCodeCellModel(cell.model)) {
      const firstLine = editor.getDoc().getLine(0);
      const secondLine = editor.getDoc().getLine(1);
      const magic = firstLine.split(' ')[0];
      if (magic.startsWith('%%') && magic in CellMagicSyntaxMap) {
        // change to whatever defined in map
        this.highlight(editor, CellMagicSyntaxMap[magic]);
        return;
      } else if (firstLine.indexOf('spark.sql(') >= 0) {
        this.highlight(editor, CellMagicSyntaxMap['%%sql']);
      } else if (secondLine !== undefined) {
        if (secondLine.indexOf('spark.sql(') >= 0) {
          this.highlight(editor, CellMagicSyntaxMap['%%sql']);
        }
      } else {
        // if not default then change to default
        this.highlight(editor, CellMagicSyntaxMap.ipython);
      }
    }
  }

  private highlight(
    cellEditor: CodeMirror.Editor,
    mode: string,
    retry = true
  ): void {
    const current_mode = cellEditor.getOption('mode') as string;
    if (current_mode === 'null') {
      if (retry) {
        // putting at the end of execution queue to allow the CodeMirror mode to be updated
        // this will be invoked as soon as possible
        setTimeout(() => this.highlight(cellEditor, mode, false), 0);
      }
      return;
    }
    if (current_mode === mode) {
      return;
    }
    cellEditor.setOption('mode', mode);
  }
}

/**
 * Activate extension
 */
function activate(app: JupyterFrontEnd, tracker: INotebookTracker): void {
  console.log('JupyterLab extension jupyterlab-sparksql is activated!');
  new SyntaxHighlighter(app, tracker);
  // console.log('SyntaxHighlighter Loaded ', sh);
}

/**
 * Initialization data for the jupyterlab_sparksql extension.
 */
const plugin: JupyterFrontEndPlugin<void> = {
  id: 'jupyterlab-sparksql:plugin',
  autoStart: true,
  requires: [INotebookTracker],
  activate: activate
};

export default plugin;
