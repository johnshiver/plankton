package terminal

import (
	"fmt"
	"strconv"
	"sync"

	"github.com/gdamore/tcell"
	"github.com/johnshiver/plankton/borg"
	"github.com/johnshiver/plankton/scheduler"
	"github.com/rivo/tview"
)

/* Slide is a function which returns the slide's main primitive and its title.
// It receives a "nextSlide" function which can be called to advance the
presentation to the next slide.
*/
type Slide func(nextSlide func()) (title string, content tview.Primitive)

var app = tview.NewApplication()

// for setting global State
var mu = sync.Mutex
var currentTaskScheduler *scheduler.TaskScheduler
var BorgScheduler *borg.BorgTaskScheduler

func SetCurrentTaskScheduler(newScheduler *scheduler.TaskScheduler) {
	mu.Lock()
	currentTaskScheduler = newScheduler
	mu.Unlock()
}

func GetCurrentTaskScheduler() *scheduler.TaskScheduler {
	mu.Lock()
	scheduler := currentTaskScheduler
	mu.Unlock()
	return scheduler
}

func GetBorgScheduler() *borg.BorgTaskScheduler {
	mu.Lock()
	bs := BorgScheduler
	mu.Unlock()
	return bs
}

// Starting point for the presentation.
func RunTerminal(bs *borg.BorgTaskScheduler) {

	BorgScheduler = bs
	SetCurrentTaskScheduler(bs.Schedulers[0])

	s := GetCurrentTaskScheduler()

	// The presentation slides.
	slides := []Slide{
		SelectScheduler,
		Logs,
	}

	// The bottom row has some info on where we are.
	info := tview.NewTextView().
		SetDynamicColors(true).
		SetRegions(true).
		SetWrap(false)

	// Create the pages for all slides.
	currentSlide := 0
	info.Highlight(strconv.Itoa(currentSlide))
	pages := tview.NewPages()
	previousSlide := func() {
		currentSlide = (currentSlide - 1 + len(slides)) % len(slides)
		info.Highlight(strconv.Itoa(currentSlide)).
			ScrollToHighlight()
		pages.SwitchToPage(strconv.Itoa(currentSlide))
	}
	nextSlide := func() {
		currentSlide = (currentSlide + 1) % len(slides)
		info.Highlight(strconv.Itoa(currentSlide)).
			ScrollToHighlight()
		pages.SwitchToPage(strconv.Itoa(currentSlide))
	}
	for index, slide := range slides {
		title, primitive := slide(nextSlide)
		pages.AddPage(strconv.Itoa(index), primitive, true, index == currentSlide)
		fmt.Fprintf(info, `%d ["%d"][darkcyan]%s[white][""]  `, index+1, index, title)
	}

	// Create the main layout.
	layout := tview.NewFlex().
		SetDirection(tview.FlexRow).
		AddItem(pages, 0, 1, true).
		AddItem(info, 1, 1, false)

	// Shortcuts to navigate the slides.
	app.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if event.Key() == tcell.KeyCtrlN {
			nextSlide()
		} else if event.Key() == tcell.KeyCtrlP {
			previousSlide()
		}
		return event
	})

	// Start the application.
	if err := app.SetRoot(layout, true).Run(); err != nil {
		panic(err)
	}
}
