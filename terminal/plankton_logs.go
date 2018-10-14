package terminal

import (
	"fmt"
	"os"

	"github.com/gdamore/tcell"
	"github.com/hpcloud/tail"
	"github.com/johnshiver/plankton/borg"
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
			t, _ := tail.TailFile(logFile,
				tail.Config{Follow: true,
					Location: &tail.SeekInfo{-1, os.SEEK_END},
					Logger:   tail.DiscardingLogger})
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
	logFile := borg.GetLogFileName()

	logFlexView := newLogView(logFile, "Borg Scheduler", nextSlide)
	return "Logs", logFlexView
}
