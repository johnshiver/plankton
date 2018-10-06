package main

import (
	"fmt"
	"strings"

	"github.com/gdamore/tcell"
	"github.com/rivo/tview"
)

const logo = `
      ___                       ___           ___           ___                       ___           ___
     /  /\                     /  /\         /__/\         /__/|          ___        /  /\         /__/\
    /  /::\                   /  /::\        \  \:\       |  |:|         /  /\      /  /::\        \  \:\
   /  /:/\:\  ___     ___    /  /:/\:\        \  \:\      |  |:|        /  /:/     /  /:/\:\        \  \:\
  /  /:/~/:/ /__/\   /  /\  /  /:/~/::\   _____\__\:\   __|  |:|       /  /:/     /  /:/  \:\   _____\__\:\
 /__/:/ /:/  \  \:\ /  /:/ /__/:/ /:/\:\ /__/::::::::\ /__/\_|:|____  /  /::\    /__/:/ \__\:\ /__/::::::::\
 \  \:\/:/    \  \:\  /:/  \  \:\/:/__\/ \  \:\~~\~~\/ \  \:\/:::::/ /__/:/\:\   \  \:\ /  /:/ \  \:\~~\~~\/
  \  \::/      \  \:\/:/    \  \::/       \  \:\  ~~~   \  \::/~~~~  \__\/  \:\   \  \:\  /:/   \  \:\  ~~~
   \  \:\       \  \::/      \  \:\        \  \:\        \  \:\           \  \:\   \  \:\/:/     \  \:\
    \  \:\       \__\/        \  \:\        \  \:\        \  \:\           \__\/    \  \::/       \  \:\
     \__\/                     \__\/         \__\/         \__\/                     \__\/         \__\/
`

const (
	subtitle   = `Simple, Fast ETLs`
	navigation = `Ctrl-N: Next Page Ctrl-P: Previous Page`
)

const tableData = `Last Run|Scheduler Name|Cron Spec
1/6/2017|Simple Scheduler|1 * * * * *
1/23/2017|Yelp Sync|0 0 * * * *
1/23/2017|Yelp Sync|0 0 * * * *
1/23/2017|Yelp Sync|0 0 * * * *
1/23/2017|Yelp Sync|0 0 * * * *
1/23/2017|Yelp Sync|0 0 * * * *
1/23/2017|Yelp Sync|0 0 * * * *
1/23/2017|Yelp Sync|0 0 * * * *
1/23/2017|Yelp Sync|0 0 * * * *
1/23/2017|Yelp Sync|0 0 * * * *
1/23/2017|Yelp Sync|0 0 * * * *
1/23/2017|Yelp Sync|0 0 * * * *
`

var SelectedTaskScheduler string

func SelectScheduler(nextSlide func()) (title string, content tview.Primitive) {
	pages := tview.NewPages()
	table := tview.NewTable().
		SetFixed(1, 1)

	list := tview.NewList()
	selectSchedulerTable := func() {
		app.SetFocus(table)
	}
	list.ShowSecondaryText(false).
		AddItem("Select Scheduler", "", '1', selectSchedulerTable).
		AddItem("Select Scheduler", "", '2', selectSchedulerTable).
		AddItem("Select Scheduler", "", '3', selectSchedulerTable).
		AddItem("Select Scheduler", "", '4', selectSchedulerTable)
	list.SetTitle("Pick an action")

	list.SetBorderPadding(1, 1, 2, 2)
	modal := tview.NewModal().
		SetText("Setting Task Scheduler").
		AddButtons([]string{"Ok"}).
		SetDoneFunc(func(buttonIndex int, buttonLabel string) {
			pages.HidePage("modal")
			app.SetFocus(list)
		})

	// populate table
	for row, line := range strings.Split(tableData, "\n") {
		for column, cell := range strings.Split(line, "|") {
			color := tcell.ColorWhite
			if row == 0 {
				color = tcell.ColorYellow
			} else if column == 0 {
				color = tcell.ColorDarkCyan
			}
			align := tview.AlignLeft
			if row == 0 {
				align = tview.AlignLeft
			} else if column == 0 || column >= 4 {
				align = tview.AlignRight
			}
			tableCell := tview.NewTableCell(cell).
				SetTextColor(color).
				SetAlign(align).
				SetSelectable(row != 0 && column != 0)
			if column >= 1 && column <= 3 {
				tableCell.SetExpansion(1)
			}
			table.SetCell(row, column, tableCell)
		}
	}
	table.SetBorder(true).SetTitle("  Assimilated Task Schedulers  ")
	table.SetBorders(false).SetSelectable(true, false).SetSeparator(' ')
	table.SetDoneFunc(func(key tcell.Key) {
		fmt.Println(key)
	}).SetSelectedFunc(func(row int, column int) {
		currCell := table.GetCell(row, 1)
		SelectedTaskScheduler = currCell.Text
		modal.SetText(fmt.Sprintf("Selected Task Scheduler: %s", SelectedTaskScheduler))
		pages.ShowPage("modal")

	})

	// What's the size of the logo?
	lines := strings.Split(logo, "\n")
	logoWidth := 0
	logoHeight := len(lines)
	for _, line := range lines {
		if len(line) > logoWidth {
			logoWidth = len(line)
		}
	}
	logoBox := tview.NewTextView().SetTextColor(tcell.ColorGreen)
	fmt.Fprint(logoBox, logo)

	navigationFrame := tview.NewFrame(tview.NewBox()).
		SetBorders(0, 0, 0, 0, 0, 0).
		AddText("", true, tview.AlignCenter, tcell.ColorWhite).
		AddText(navigation, true, tview.AlignCenter, tcell.ColorBlue)

	// Create a Flex layout that centers the logo and subtitle.
	mainPageFlex := tview.NewFlex().
		SetDirection(tview.FlexRow).
		AddItem(tview.NewFlex().
			AddItem(tview.NewBox(), 0, 1, false).
			AddItem(logoBox, logoWidth, 1, true).
			AddItem(tview.NewBox(), 0, 1, false), logoHeight, 1, false).
		AddItem(navigationFrame, 0, 1, false).
		AddItem(tview.NewFlex().SetDirection(tview.FlexColumn).
			AddItem(table, 0, 3, false).
			AddItem(list, 0, 1, true),
			0, 2, true)

	pages.AddPage("mainPage", mainPageFlex, true, true)
	pages.AddPage("modal", modal, false, false)

	return "Select Schdeuler", pages
}
