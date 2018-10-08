package terminal

import (
	"fmt"
	"os"

	"github.com/gdamore/tcell"
	"github.com/hpcloud/tail"
	"github.com/rivo/tview"
)

// creates a new full page log view for given logFile
// doneFunc is the function that is called when the view receives the Done Event
func newLogView(logFile, logTitle string, doneFunc func()) *tview.Flex {
	textView := tview.NewTextView().
		SetTextColor(tcell.ColorGreen).
		SetScrollable(true).
		SetChangedFunc(func() {
			app.Draw()
		}).SetDoneFunc(func(key tcell.Key) {
		doneFunc()
	})
	go func() {
		for {
			t, err := tail.TailFile(logFile,
				tail.Config{Follow: true,
					Location: &tail.SeekInfo{-1, os.SEEK_END}})
			if err != nil {
				panic(err)
			}
			for line := range t.Lines {
				fmt.Fprintf(textView, "%s\n", line.Text)
			}
		}
	}()
	finalLogTitle := fmt.Sprintf(" %s - Logs ", logTitle)
	textView.SetBorder(true).SetTitle(finalLogTitle)

	logFlexView := tview.NewFlex().
		SetDirection(tview.FlexRow).
		AddItem(textView, 0, 1, true)

	return logFlexView

}

func Logs(nextSlide func()) (title string, content tview.Primitive) {

	logFlexView := newLogView("/home/john/./.plankton_logs/plankton_borg.log", "Borg Scheduler", nextSlide)
	return "Logs", logFlexView
}
