package main

import (
	"fmt"
	"os"

	"github.com/gdamore/tcell"
	"github.com/hpcloud/tail"
	"github.com/rivo/tview"
)

func Logs(nextSlide func()) (title string, content tview.Primitive) {

	textView := tview.NewTextView().
		SetTextColor(tcell.ColorGreen).
		SetScrollable(true).
		SetChangedFunc(func() {
			app.Draw()
		}).SetDoneFunc(func(key tcell.Key) {
		nextSlide()
	})
	go func() {
		for {
			t, err := tail.TailFile("/home/jshiver/.plankton_logs/simple scheduler-scheduler.log",
				tail.Config{Follow: true,
					Location: &tail.SeekInfo{-1, os.SEEK_END}})
			if err != nil {
				panic(err)
			}
			for line := range t.Lines {
				//	fmt.Println(line.Text)
				fmt.Fprintf(textView, "%s\n", line.Text)
			}
		}
	}()
	textView.SetBorder(true).SetTitle("Logs for Scheduler")

	flex := tview.NewFlex().
		SetDirection(tview.FlexRow).
		// AddItem(tview.NewBox(), 0, 7, false).
		AddItem(tview.NewFlex().
			// AddItem(tview.NewBox(), 0, 1, false).
			AddItem(textView, 0, 1, true), 100, 1, true)
		//AddItem(tview.NewBox(), 0, 1, false) 100, 1, true)

	return "Logs", flex
}
