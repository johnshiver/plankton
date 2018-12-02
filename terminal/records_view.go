package terminal

import (
	"fmt"
	"strings"

	"github.com/gdamore/tcell"
	"github.com/johnshiver/plankton/config"
	"github.com/rivo/tview"
)

var selectedUUID string

func newRecordTableView(doneFunc func()) tview.Primitive {

	actionList := tview.NewList()
	tableView := createRecordsTableView(actionList)
	configureActionList(actionList, tableView, doneFunc)

	// Create a Flex layout that centers the logo and subtitle.
	mainPageFlex := tview.NewFlex().
		SetDirection(tview.FlexRow).
		AddItem(tview.NewFlex().SetDirection(tview.FlexColumn).
			AddItem(tableView, 0, 3, false).
			AddItem(actionList, 0, 1, true),
			0, 2, true)

	return mainPageFlex
}

type result struct {
	TaskParams string
}

func setTableCells(table *tview.Table) {
	cTaskScheduler := GetCurrentTaskScheduler()
	tableData := []string{}
	tableData = append(tableData, "SchedulerUUID|Start|End|Version|TaskParams")
	for _, r := range cTaskScheduler.LastRecords() {
		results := []result{}
		c := config.GetConfig()
		c.DataBase.Table("plankton_records").
			Select("task_params").
			Where("scheduler_uuid = ?", r.SchedulerUUID).
			Order("ended_at desc").
			Scan(&results)
		taskParams := []string{}
		parmCheck := make(map[string]int)
		for _, n := range results {
			parmCheck[n.TaskParams] += 1
			if parmCheck[n.TaskParams] == 1 {
				taskParams = append(taskParams, n.TaskParams)
			}
		}
		line := fmt.Sprintf("%s|%s|%s|%s|%s", r.SchedulerUUID, r.Start[:19], r.End[:19], r.Version, strings.Join(taskParams, " "))
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
			alignment := tview.AlignLeft
			tableCell := tview.NewTableCell(cell).
				SetTextColor(color).
				SetAlign(alignment).
				SetSelectable(row != 0)
			if column >= 1 {
				tableCell.SetExpansion(1)
			}
			table.SetCell(row, column, tableCell)
		}
	}

}

func createRecordsTableView(actionList *tview.List) *tview.Table {
	table := tview.NewTable().
		SetFixed(1, 1)
	setTableCells(table)
	cTaskScheduler := GetCurrentTaskScheduler()
	tableSelectRow := func(row int, column int) {
		uuidColumn := 0
		currCell := table.GetCell(row, uuidColumn)
		selectedUUID = currCell.Text
	}
	// style the table and set props  ---------------------------------------------------
	tableTitle := fmt.Sprintf(" %s Records ", cTaskScheduler.Name)
	table.SetBorder(true).SetTitle(tableTitle)
	table.SetBorders(false).SetSelectable(true, false).SetSeparator(' ')
	table.SetSelectedFunc(tableSelectRow)
	table.SetDoneFunc(func(key tcell.Key) {
		if key == tcell.KeyEsc {
			app.SetFocus(actionList)
		}

	})
	return table
}

func configureActionList(actionList *tview.List, table *tview.Table, doneFunc func()) *tview.List {
	selectRecordTable := func() {
		app.SetFocus(table)
	}

	reRunTask := func() {
		cTaskScheduler := GetCurrentTaskScheduler()
		go cTaskScheduler.ReRun(selectedUUID)
	}

	actionList.ShowSecondaryText(false).
		AddItem("Select Record", "", '1', selectRecordTable).
		AddItem("Rerun", "", '2', reRunTask).
		AddItem("Return", "", '3', doneFunc)
	actionList.SetTitleColor(tcell.ColorWhite)
	actionList.SetTitle(" Pick an action ")

	actionList.SetBorderPadding(1, 1, 2, 2)

	return actionList
}
