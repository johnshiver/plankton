package terminal

import (
	"fmt"
	"strings"

	"github.com/gdamore/tcell"
	"github.com/johnshiver/plankton/scheduler"
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
	navigation = `Select a Task Scheduler then choose an action on the right`
	MAIN_PAGE  = "main_page"
	MODAL_PAGE = "modal_page"
)

func CreateSelectSchedulerView() tview.Primitive {

	bs := GetBorgScheduler()
	SetCurrentTaskScheduler(bs.Schedulers[0].Scheduler)

	pages := tview.NewPages()
	selectionModal := tview.NewModal().
		SetText("Setting Task Scheduler").
		AddButtons([]string{"Ok"})
	pages.AddPage(MODAL_PAGE, selectionModal, false, false)

	table := CreateTableView(selectionModal, pages)
	actionList := CreateActionList(table, pages)

	selectionModal.SetDoneFunc(func(buttonIndex int, buttonLabel string) {
		pages.HidePage(MODAL_PAGE)
		app.SetFocus(actionList)
	})

	//  -------------------------- Create page header + logo

	// Figure out logo dimensions
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
			AddItem(actionList, 0, 1, true),
			0, 2, true)
	pages.AddPage(MAIN_PAGE, mainPageFlex, true, true)

	return pages
}

func CreateTableView(selectionModal *tview.Modal, pages *tview.Pages) *tview.Table {
	table := tview.NewTable().
		SetFixed(1, 1)

	// populate table from scheduler ---------------------------------------------------
	bs := GetBorgScheduler()
	tableData := []string{}
	tableData = append(tableData, "Last Run|Scheduler Name|Cron Spec")
	for _, tScheduler := range bs.Schedulers {
		line := fmt.Sprintf("%s|%s|%s", tScheduler.Scheduler.LastRun(), tScheduler.Scheduler.Name, tScheduler.ScheduleSpec)
		tableData = append(tableData, line)
	}
	for row, line := range tableData {
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

	tableSelectRow := func(row int, column int) {
		currCell := table.GetCell(row, 1)
		SelectedTaskScheduler := currCell.Text
		for _, aScheduler := range bs.Schedulers {
			if aScheduler.Scheduler.Name == SelectedTaskScheduler {
				SetCurrentTaskScheduler(aScheduler.Scheduler)
				break
			}
		}
		selectionModal.SetText(fmt.Sprintf("Selected Task Scheduler: %s", currentTaskScheduler.Name))
		pages.ShowPage(MODAL_PAGE)

	}

	// style the table and set props  ---------------------------------------------------
	table.SetBorder(true).SetTitle("  Assimilated Task Schedulers  ")
	table.SetBorders(false).SetSelectable(true, false).SetSeparator(' ')
	table.SetDoneFunc(func(key tcell.Key) {
		fmt.Println(key)
	}).SetSelectedFunc(tableSelectRow)

	return table

}

func CreateActionList(table *tview.Table, pages *tview.Pages) *tview.List {
	actionList := tview.NewList()
	selectSchedulerTable := func() {
		app.SetFocus(table)
	}

	logViewDoneFunc := func() {
		pages.ShowPage(MAIN_PAGE)
		app.SetFocus(actionList)
		pages.RemovePage(currentTaskScheduler.Name)
	}

	showSchedulerLogs := func() {
		schedulerLogFile := scheduler.GetTaskSchedulerLogFilePath(currentTaskScheduler.Name)
		newLogView := newLogView(schedulerLogFile, currentTaskScheduler.Name, logViewDoneFunc)
		pages.AddPage(currentTaskScheduler.Name, newLogView, true, true)
		pages.ShowPage(currentTaskScheduler.Name)
	}

	actionList.ShowSecondaryText(false).
		AddItem("Select Scheduler", "", '1', selectSchedulerTable).
		AddItem("Show Scheduler Logs", "", '2', showSchedulerLogs)
	actionList.SetTitleColor(tcell.ColorWhite)
	actionList.SetTitle(" Pick an action ")

	actionList.SetBorderPadding(1, 1, 2, 2)

	return actionList
}
